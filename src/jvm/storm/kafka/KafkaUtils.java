package storm.kafka;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.trident.FailedFetchException;
import storm.kafka.trident.TridentKafkaConfig;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tigerby on 2/26/14.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class KafkaUtils {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);

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
                LOG.error("Error communicating with Broker [{}] to find Leader for [{}, {}] Reason: ", seed, topic, partition, e);
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }

        return returnMetaData;
    }

    public static List<HostPort> replicas(PartitionMetadata metadata) {
        List<HostPort> replicaBrokers = new ArrayList<HostPort>();
        for (Broker broker : metadata.replicas()) {
            replicaBrokers.add(new HostPort(broker.host(), broker.port()));
        }
        return replicaBrokers;
    }

    public static HostPort findNewLeader(List<HostPort> replicaBrokers, HostPort oldLeader, String topic, int partition) {
        // TODO: property retry count.
        for (int i = 0; i < 10; i++) {
            LOG.info("trying to find new leader: {} times", i + 1);

            boolean goToSleep;
            PartitionMetadata metadata = findLeader(replicaBrokers, topic, partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (!oldLeader.host.equals(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                goToSleep = true;
            } else {
                HostPort newLeader = new HostPort(metadata.leader().host(), metadata.leader().port());
                LOG.info("New leader found is {}", newLeader);
                return newLeader;
            }

            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        throw new RuntimeException("Unable to find new leader after Broker failure. Exiting");
    }

    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            LOG.error("Error fetching data Offset Data the Broker. Reason: {}, set Offset to 0.", ErrorMapping.exceptionFor(response.errorCode(topic, partition)));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

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

}
