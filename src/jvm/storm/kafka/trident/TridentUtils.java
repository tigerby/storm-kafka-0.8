package storm.kafka.trident;

import java.util.*;

import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.ReducedMetric;
import com.google.common.collect.ImmutableMap;

import backtype.storm.utils.Utils;
import kafka.api.OffsetRequest;
import kafka.common.ErrorMapping;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;
import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.KafkaConfig.ZkHosts;
import storm.trident.operation.TridentCollector;

public class TridentUtils {

    public static final Logger LOG = LoggerFactory.getLogger(TridentUtils.class);

    public static IBrokerReader makeBrokerReader(Map stormConf, TridentKafkaConfig conf) {
        if (conf.hosts instanceof StaticHosts) {
            return new StaticBrokerReader(conf);
        } else {
            return new ZkBrokerReader(stormConf, conf.topic, (ZkHosts) conf.hosts);
        }
    }

    public static Map emitPartitionBatchNew(TridentKafkaConfig config, SimpleConsumer consumer,
                                            GlobalPartitionId partition, TridentCollector collector,
                                            Map lastMeta, String topologyInstanceId,
                                            String topologyName,
                                            final ReducedMetric meanMetric, final CombinedMetric maxMetric) {
        long offset;
        if (lastMeta != null) {
            String lastInstanceId = null;
            Map lastTopoMeta = (Map) lastMeta.get("topology");
            if (lastTopoMeta != null) {
                lastInstanceId = (String) lastTopoMeta.get("id");
            }
            if (config.forceFromStart && !topologyInstanceId.equals(lastInstanceId)) {
                offset = KafkaUtils.getLastOffset(consumer, config.topic, partition.partition, config.startOffsetTime,
                        KafkaUtils.makeClientName(config.topic, partition.partition));
            } else {
                offset = (Long) lastMeta.get("nextOffset");
            }
        } else {
            long startTime = -1;
            if (config.forceFromStart) {
                startTime = config.startOffsetTime;
            }
            offset = KafkaUtils.getLastOffset(consumer, config.topic, partition.partition, startTime,
                    KafkaUtils.makeClientName(config.topic, partition.partition));
        }

        FetchResponse fetchResponse = KafkaUtils.fetch(consumer, config, partition, offset, new FetchHandler() {
            @Override
            public void updateMetrics(long latency) {
                meanMetric.update(latency);
                maxMetric.update(latency);
            }
        });

        if (fetchResponse.hasError()) {
            short code = fetchResponse.errorCode(config.topic, partition.partition);

            LOG.warn("Error fetching data from the Broker: {}, Reason: {}", partition, ErrorMapping.exceptionFor(code));

            if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                // For simple case ask for the last element to reset
                long lastOffset = KafkaUtils.getLastOffset(consumer, config.topic, partition.partition, OffsetRequest.LatestTime(),
                        KafkaUtils.makeClientName(config.topic, partition.partition));

                LOG.info("set next offset from {} to {}", offset, lastOffset);

                lastMeta.put("nextOffset", lastOffset);
                return lastMeta;
            }

            // TODO: update consumer to new leader and replicas, then return new last meta.
        }

        ByteBufferMessageSet msgSet = fetchResponse.messageSet(config.topic, partition.partition);

        if(msgSet.validBytes() != msgSet.sizeInBytes()) {
            LOG.warn("Fetched data with no error, valid bytes({}) is not same as size in bytes({}) on {}.", msgSet.validBytes(), msgSet.sizeInBytes(), partition);
        }

        long readOffset = offset;
        for (MessageAndOffset msg : msgSet) {
            long currentOffset = msg.offset();

            if (currentOffset < readOffset) {
                LOG.warn("Found an old offset: {} Expecting: {}", currentOffset, offset);
                continue;
            }

            emit(config, collector, msg.message());
            readOffset = msg.nextOffset();
        }

        Map newMeta = new HashMap();
        newMeta.put("offset", offset);
        newMeta.put("nextOffset", readOffset);
        newMeta.put("instanceId", topologyInstanceId);
        newMeta.put("partition", partition.partition);
        newMeta.put("broker", ImmutableMap.of("host", partition.host.host, "port", partition.host.port));
        newMeta.put("topic", config.topic);
        newMeta.put("topology", ImmutableMap.of("name", topologyName, "id", topologyInstanceId));

        return newMeta;
    }

    public static void emit(TridentKafkaConfig config, TridentCollector collector, Message msg) {
        Iterable<List<Object>> values = config.scheme.deserialize(Utils.toByteArray(msg.payload()));
        if (values != null) {
            for (List<Object> value : values) {
                collector.emit(value);
            }
        }
    }


}
