package storm.kafka;

import backtype.storm.Config;
import backtype.storm.metric.api.*;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;

import com.google.common.collect.ImmutableMap;

import java.util.*;

import kafka.javaapi.PartitionMetadata;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.KafkaSpout.EmitState;
import storm.kafka.KafkaSpout.MessageAndRealOffset;
import storm.kafka.metric.MaxMetric;

public class PartitionManager {

    public static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);

    private final CombinedMetric _fetchAPILatencyMax;
    private final ReducedMetric _fetchAPILatencyMean;
    private final CountMetric _fetchAPICallCount;
    private final CountMetric _fetchAPIMessageCount;

    static class KafkaMessageId {

        public GlobalPartitionId partition;
        public long offset;

        public KafkaMessageId(GlobalPartitionId partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }

    Long _emittedToOffset;
    SortedSet<Long> _pending = new TreeSet<Long>();
    Long _committedTo;
    LinkedList<MessageAndRealOffset> _waitingToEmit = new LinkedList<MessageAndRealOffset>();
    GlobalPartitionId partitionId;
    SpoutConfig _spoutConfig;
    String _topologyInstanceId;
    SimpleConsumer _consumer;
    DynamicPartitionConnections _connections;
    ZkState _state;
    Map _stormConf;
    List<HostPort> replicaBrokers = new ArrayList<HostPort>();
    PartitionCoordinator coordinator;


    public PartitionManager(DynamicPartitionConnections connections, String topologyInstanceId,
                            ZkState state, Map stormConf, SpoutConfig spoutConfig,
                            GlobalPartitionId id, List<HostPort> replicaBrokers, PartitionCoordinator coordinator) {
        partitionId = id;
        _connections = connections;
        _spoutConfig = spoutConfig;
        _topologyInstanceId = topologyInstanceId;
        _consumer = connections.register(id.host, id.partition);
        _state = state;
        _stormConf = stormConf;
        this.replicaBrokers = replicaBrokers;
        this.coordinator = coordinator;

        String jsonTopologyId = null;
        Long jsonOffset = null;
        try {
            Map<Object, Object> json = _state.readJSON(committedPath());
            if (json != null) {
                jsonTopologyId = (String) ((Map<Object, Object>) json.get("topology")).get("id");
                jsonOffset = (Long) json.get("offset");
            }
        } catch (Throwable e) {
            LOG.warn("Error reading and/or parsing at ZkNode: " + committedPath(), e);
        }

        if (!topologyInstanceId.equals(jsonTopologyId) && spoutConfig.forceFromStart) {
            _committedTo = KafkaUtils.getLastOffset(_consumer, spoutConfig.topic, id.partition, spoutConfig.startOffsetTime, "cli-" + spoutConfig.topic + "-" + id.partition);
            LOG.info("Init offset: [{}] Using startOffsetTime to choose last commit offset {}.", partitionId, _committedTo);
        } else if (jsonTopologyId == null || jsonOffset == null) { // failed to parse JSON?
            _committedTo = KafkaUtils.getLastOffset(_consumer, spoutConfig.topic, id.partition, kafka.api.OffsetRequest.LatestTime(), "cli-" + spoutConfig.topic + "-" + id.partition);
            LOG.info("Init offset: [{}] Setting last commit offset to HEAD({}).", partitionId, _committedTo);
        } else {
            _committedTo = jsonOffset;
            LOG.info("Init offset: [{}] Read last commit offset from zookeeper: {}", partitionId, _committedTo);
        }

        LOG.info("Starting Kafka {} from offset {}", partitionId, _committedTo);
        _emittedToOffset = _committedTo;

        _fetchAPILatencyMax = new CombinedMetric(new MaxMetric());
        _fetchAPILatencyMean = new ReducedMetric(new MeanReducer());
        _fetchAPICallCount = new CountMetric();
        _fetchAPIMessageCount = new CountMetric();
    }

    public Map getMetricsDataMap() {
        Map ret = new HashMap();
        ret.put(partitionId + "/fetchAPILatencyMax", _fetchAPILatencyMax.getValueAndReset());
        ret.put(partitionId + "/fetchAPILatencyMean", _fetchAPILatencyMean.getValueAndReset());
        ret.put(partitionId + "/fetchAPICallCount", _fetchAPICallCount.getValueAndReset());
        ret.put(partitionId + "/fetchAPIMessageCount", _fetchAPIMessageCount.getValueAndReset());
        return ret;
    }

    //returns false if it's reached the end of current batch
    public EmitState next(SpoutOutputCollector collector) {
        if (_waitingToEmit.isEmpty()) fill();

        while (true) {
            MessageAndRealOffset toEmit = _waitingToEmit.pollFirst();
            if (toEmit == null) {
                return EmitState.NO_EMITTED;
            }
            Iterable<List<Object>>
                    tups = _spoutConfig.scheme.deserialize(Utils.toByteArray(toEmit.msg.payload()));
            if (tups != null) {
                for (List<Object> tup : tups) {
                    collector.emit(tup, new KafkaMessageId(partitionId, toEmit.offset));
                }
                break;
            } else {
                ack(toEmit.offset);
            }
        }
        if (!_waitingToEmit.isEmpty()) {
            return EmitState.EMITTED_MORE_LEFT;
        } else {
            return EmitState.EMITTED_END;
        }
    }

    private void fill() {
        FetchResponse fetchResponse = KafkaUtils.fetch(_consumer, _spoutConfig, partitionId, _emittedToOffset, new FetchHandler() {
            @Override
            public void updateMetrics(long latency) {
                _fetchAPILatencyMax.update(latency);
                _fetchAPILatencyMean.update(latency);
            }
        });

        if(fetchResponse == null) {
            LOG.error("Error fetching data from the Broker: {}, Reason: fetch response is null", partitionId);
            updateConsumer();
            return;
        }

        if (fetchResponse.hasError()) {
            short code = fetchResponse.errorCode(_spoutConfig.topic, partitionId.partition);

            LOG.error("Error fetching data from the Broker: {}, Reason: {}", partitionId, ErrorMapping.exceptionFor(code));

            if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                // We asked for an invalid offset. For simple case ask for the last element to reset
                _emittedToOffset = KafkaUtils.getLastOffset(_consumer, _spoutConfig.topic, partitionId.partition, kafka.api.OffsetRequest.LatestTime(),
                        KafkaUtils.makeClientName(_spoutConfig.topic, partitionId.partition));
                return;
            }

            updateConsumer();
            return;
        }

        ByteBufferMessageSet msgSet = fetchResponse.messageSet(_spoutConfig.topic, partitionId.partition);

        int numMessages = msgSet.sizeInBytes();

        _fetchAPICallCount.incr();
        _fetchAPIMessageCount.incrBy(numMessages);

        if (numMessages > 0) {
            LOG.debug("Fetched {} byte messages from Kafka: {}", numMessages, partitionId);
        }
        for (MessageAndOffset msg : msgSet) {
            long currentOffset = msg.offset();

            if (currentOffset < _emittedToOffset) {
                LOG.warn("Found an old offset: {} Expecting: {}", currentOffset, _emittedToOffset);
                continue;
            }

            _pending.add(_emittedToOffset);
            _waitingToEmit.add(new MessageAndRealOffset(msg.message(), _emittedToOffset));
            _emittedToOffset = msg.nextOffset();
        }
        if (numMessages > 0) {
            LOG.debug("Added {} byte messages from Kafka: {} to internal buffers", numMessages, partitionId);
        }
    }

    private void updateConsumer() {
        LOG.info("reset this partition: {}", partitionId);

        PartitionMetadata metadata = KafkaUtils.getPartitionMetadata(replicaBrokers, _spoutConfig.topic, partitionId.partition);

        if(metadata == null) return;

        HostPort newLeader = new HostPort(metadata.leader().host(), metadata.leader().port());

        // update global partition ID of coordinator
        coordinator.updatePartitionId(partitionId, new GlobalPartitionId(newLeader, partitionId.partition));

        // update metadata
        _connections.unregister(partitionId.host, partitionId.partition);
        if(_consumer != null)   _consumer.close();

        partitionId.host = newLeader;
        _consumer = _connections.register(newLeader, partitionId.partition);
        replicaBrokers = KafkaUtils.replicas(metadata);

        LOG.info("New leader found is {}. partition: {}", newLeader, partitionId.partition);
    }

    public void ack(Long offset) {
        _pending.remove(offset);
    }

    public void fail(Long offset) {
        //TODO: should it use in-memory ack set to skip anything that's been acked but not committed???
        // things might get crazy with lots of timeouts
        if (_emittedToOffset > offset) {
            _emittedToOffset = offset;
            _pending.tailSet(offset).clear();
        }
    }

    public void commit() {
        LOG.debug("Committing offset {} for {}", _committedTo, partitionId);
        long committedTo;
        if (_pending.isEmpty()) {
            committedTo = _emittedToOffset;
        } else {
            committedTo = _pending.first();
        }
        if (committedTo != _committedTo) {
            LOG.debug("Writing committed offset to ZK: {}", committedTo);

            Map<Object, Object> data = ImmutableMap.builder()
                    .put("topology", ImmutableMap.of("id", _topologyInstanceId, "name", _stormConf.get(Config.TOPOLOGY_NAME)))
                    .put("offset", committedTo)
                    .put("partition", partitionId.partition)
                    .put("broker", ImmutableMap.of("host", partitionId.host.host, "port", partitionId.host.port))
                    .put("topic", _spoutConfig.topic).build();

            _state.writeJSON(committedPath(), data);

            LOG.debug("Wrote committed offset to ZK: {}", committedTo);
            _committedTo = committedTo;
        }
        LOG.info("Committed offset {} for {}", committedTo, partitionId);
    }

    private String committedPath() {
        return _spoutConfig.zkRoot + "/" + _spoutConfig.id + "/" + partitionId;
    }

    public long lastCommittedOffset() {
        return _committedTo;
    }

    public long lastCompletedOffset() {
        if (_pending.isEmpty()) {
            return _emittedToOffset;
        } else {
            return _pending.first();
        }
    }

    public GlobalPartitionId getPartition() {
        return partitionId;
    }

    public void close() {
        _connections.unregister(partitionId.host, partitionId.partition);
    }
}
