package storm.kafka.metric;

import backtype.storm.metric.api.IMetric;
import kafka.api.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import storm.kafka.DynamicPartitionConnections;
import storm.kafka.GlobalPartitionId;
import storm.kafka.KafkaUtils;
import storm.kafka.trident.TridentUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
* Created by tigerby on 2/26/14.
*
* @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
* @version 1.0
*/
public class KafkaOffsetMetric implements IMetric {

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

                    long latestTimeOffset = KafkaUtils.getLastOffset(consumer, _topic, partition.partition, OffsetRequest.LatestTime(),
                            KafkaUtils.makeClientName(_topic, partition.partition));

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
