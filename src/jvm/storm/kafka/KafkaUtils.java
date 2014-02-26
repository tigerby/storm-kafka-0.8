package storm.kafka;

import backtype.storm.metric.api.IMetric;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.trident.FailedFetchException;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.trident.TridentUtils;

import java.net.ConnectException;
import java.util.*;

/**
 * Created by tigerby on 2/26/14.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class KafkaUtils {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);

    public static FetchResponse fetch(SimpleConsumer consumer, TridentKafkaConfig config, GlobalPartitionId partition, long offset, FetchHandler handler) {
        FetchResponse fetchResponse;
        try {
            long start = System.nanoTime();
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(makeClientName(config.topic, partition.partition))
                    .addFetch(config.topic, partition.partition, offset, config.fetchSizeBytes)
                    .build();

            fetchResponse = consumer.fetch(req);
            long end = System.nanoTime();
            long millis = (end - start) / 1000000;

            handler.updateMetrics(millis);
        } catch (Exception e) {
            if (e instanceof ConnectException) {
                throw new FailedFetchException(e);
            } else {
                throw new RuntimeException(e);
            }
        }

        return fetchResponse;
    }

    public static String makeClientName(String topic, int partition) {
        return "cli-" + topic + "-" + partition;
    }

    public static PartitionMetadata findLeader(List<HostPort> seedBrokers, String topic, int partition) {
        PartitionMetadata returnMetaData = null;
        for (HostPort seed : seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                // TODO: property meta data.
                consumer = new SimpleConsumer(seed.host, seed.port, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = new ArrayList<String>();
                topics.add(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnMetaData = part;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                TridentUtils.LOG.error("Error communicating with Broker [{}] to find Leader for [{}, {}] Reason: ", seed, topic, partition, e);
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }

        return returnMetaData;
    }

    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            TridentUtils.LOG.error("Error fetching data Offset Data the Broker. Reason: {}, set Offset to 0.", ErrorMapping.exceptionFor(response.errorCode(topic, partition)));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    public static class KafkaOffsetMetric implements IMetric {

        Map<GlobalPartitionId, Long> _partitionToOffset = new HashMap<GlobalPartitionId, Long>();
        Set<GlobalPartitionId> _partitions;
        String _topic;
        DynamicPartitionConnections _connections;

        public KafkaOffsetMetric(String topic, DynamicPartitionConnections connections) {
            _topic = topic;
            _connections = connections;
        }

        public void setLatestEmittedOffset(GlobalPartitionId partition, long offset) {
            _partitionToOffset.put(partition, offset);
        }

        @Override
        public Object getValueAndReset() {
            try {
                long totalSpoutLag = 0;
                long totalLatestTimeOffset = 0;
                long totalLatestEmittedOffset = 0;
                HashMap ret = new HashMap();
                if (_partitions != null && _partitions.size() == _partitionToOffset.size()) {
                    for (Map.Entry<GlobalPartitionId, Long> e : _partitionToOffset.entrySet()) {
                        GlobalPartitionId partition = e.getKey();
                        SimpleConsumer consumer = _connections.getConnection(partition);

                        if (consumer == null) {
                            TridentUtils.LOG.warn("partitionToOffset contains partition not found in _connections. Stale partition data?");
                            return null;
                        }

                        long latestTimeOffset = getLastOffset(consumer, _topic, partition.partition, OffsetRequest.LatestTime(),
                                makeClientName(_topic, partition.partition));

                        if (latestTimeOffset == 0) {
                            TridentUtils.LOG.warn("No data found in Kafka Partition " + partition.getId());
                            return null;
                        }

                        long latestEmittedOffset = (Long) e.getValue();
                        long spoutLag = latestTimeOffset - latestEmittedOffset;

                        ret.put(partition.getId() + "/" + "spoutLag", spoutLag);
                        ret.put(partition.getId() + "/" + "latestTime", latestTimeOffset);
                        ret.put(partition.getId() + "/" + "latestEmittedOffset", latestEmittedOffset);

                        totalSpoutLag += spoutLag;
                        totalLatestTimeOffset += latestTimeOffset;
                        totalLatestEmittedOffset += latestEmittedOffset;
                    }

                    ret.put("totalSpoutLag", totalSpoutLag);
                    ret.put("totalLatestTime", totalLatestTimeOffset);
                    ret.put("totalLatestEmittedOffset", totalLatestEmittedOffset);
                    return ret;
                } else {
                    TridentUtils.LOG.info("Metrics Tick: Not enough data to calculate spout lag.");
                }
            } catch (Throwable t) {
                TridentUtils.LOG.warn("Metrics Tick: Exception when computing kafkaOffset metric.", t);
            }
            return null;
        }

        public void refreshPartitions(Set<GlobalPartitionId> partitions) {
            _partitions = partitions;
            Iterator<GlobalPartitionId> it = _partitionToOffset.keySet().iterator();
            while (it.hasNext()) {
                if (!partitions.contains(it.next())) {
                    it.remove();
                }
            }
        }
    }
}
