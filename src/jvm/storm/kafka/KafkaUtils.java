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

    public static PartitionMetadata getPartitionMetadata(List<HostPort> seedBrokers, String topic, int partition) {
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
                LOG.warn("Cannot communicating with Broker {} to get partition metadata for {}:{} Reason: {}", seed, topic, partition, e.getMessage());
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }

            if(returnMetaData != null) break;
        }

        if(returnMetaData == null) LOG.error("Partition metadata does not existed. [{}, {}]", topic, partition);

        return returnMetaData;
    }

    public static List<HostPort> replicas(PartitionMetadata metadata) {
        List<HostPort> replicaBrokers = new ArrayList<HostPort>();
        for (Broker broker : metadata.replicas()) {
            replicaBrokers.add(new HostPort(broker.host(), broker.port()));
        }
        return replicaBrokers;
    }

    @Deprecated
    public static PartitionMetadata recoverPartitionMetadata(List<HostPort> replicaBrokers, HostPort oldLeader, String topic, int partition) {
        LOG.info("trying to find new leader.");

        PartitionMetadata metadata = getPartitionMetadata(replicaBrokers, topic, partition);
        if (metadata != null && metadata.leader() != null &&
                !(oldLeader.host.equals(metadata.leader().host()) && oldLeader.port == metadata.leader().port())) {
            return metadata;
        }

        return null;
    }

    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, OffsetRequest.CurrentVersion(), clientName);

        OffsetResponse response = null;
        try {
            response = consumer.getOffsetsBefore(request);
        } catch (Exception e) {
            LOG.error("Error occured when getting last offset. Reason: {}\nset offset 0.", e.getMessage());
            return 0;
        }

        if (response.hasError()) {
            LOG.error("Error occured when getting last offset. Reason: {}\nset offset 0.", ErrorMapping.exceptionFor(response.errorCode(topic, partition)));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    public static FetchResponse fetch(SimpleConsumer consumer, KafkaConfig config, GlobalPartitionId partition, long offset, FetchHandler handler) {
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
            LOG.error("Error occured while fetching data. partition: {}, offset: {}, Reason: {}", partition, offset, e.getMessage());
            return null;
        }

        return fetchResponse;
    }

    public static String makeClientName(String topic, int partition) {
        return "cli-" + topic + "-" + partition;
    }

}
