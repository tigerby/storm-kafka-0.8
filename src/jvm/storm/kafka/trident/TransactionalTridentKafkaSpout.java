package storm.kafka.trident;

import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

import java.util.*;

import com.google.common.collect.ImmutableMap;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.DynamicPartitionConnections;
import storm.kafka.FetchHandler;
import storm.kafka.GlobalPartitionId;
import storm.kafka.KafkaUtils;
import storm.kafka.metric.KafkaOffsetMetric;
import storm.kafka.metric.MaxMetric;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;


public class TransactionalTridentKafkaSpout implements IPartitionedTridentSpout<List<GlobalPartitionId>, GlobalPartitionId, Map> {

    TridentKafkaConfig _config;
    String _topologyInstanceId = UUID.randomUUID().toString();
    public static final Logger LOG = LoggerFactory.getLogger(TransactionalTridentKafkaSpout.class);

    public TransactionalTridentKafkaSpout(TridentKafkaConfig config) {
        _config = config;
    }

    class Coordinator implements IPartitionedTridentSpout.Coordinator<List<GlobalPartitionId>> {
        private final TopologyContext context;
        IBrokerReader reader;

        public Coordinator(Map conf, TopologyContext context) {
            reader = TridentUtils.makeBrokerReader(conf, _config);
            this.context = context;
        }

        @Override
        public void close() {
            _config.coordinator.close();
        }

        @Override
        public boolean isReady(long txid) {
            return _config.coordinator.isReady(txid);
        }

        @Override
        public List<GlobalPartitionId> getPartitionsForBatch() {
            return reader.getCurrentBrokers();
        }
    }

    class Emitter implements IPartitionedTridentSpout.Emitter<List<GlobalPartitionId>, GlobalPartitionId, Map> {
        DynamicPartitionConnections _connections;
        String _topologyName;
        TopologyContext _context;
        KafkaOffsetMetric _kafkaOffsetMetric;
        ReducedMetric _kafkaMeanFetchLatencyMetric;
        CombinedMetric _kafkaMaxFetchLatencyMetric;

        public Emitter(Map conf, TopologyContext context) {
            _connections = new DynamicPartitionConnections(_config);
            _topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
            _context = context;
            _kafkaOffsetMetric = new KafkaOffsetMetric(_config.topic, _connections);
            context.registerMetric("kafkaOffset", _kafkaOffsetMetric, 60);
            _kafkaMeanFetchLatencyMetric = context.registerMetric("kafkaFetchAvg", new MeanReducer(), 60);
            _kafkaMaxFetchLatencyMetric = context.registerMetric("kafkaFetchMax", new MaxMetric(), 60);
        }

        @Override
        public Map emitPartitionBatchNew(TransactionAttempt attempt, TridentCollector collector, GlobalPartitionId partition, Map lastMeta) {
//            LOG.info("[{}] {}, {}, {}", _context.getThisTaskId(), attempt, partition, lastMeta);
            SimpleConsumer consumer = _connections.register(partition);
            Map ret = TridentUtils.emitPartitionBatchNew(_config, consumer, partition, collector, lastMeta, _topologyInstanceId, _topologyName, _kafkaMeanFetchLatencyMetric, _kafkaMaxFetchLatencyMetric);
            _kafkaOffsetMetric.setLatestEmittedOffset(partition, (Long)ret.get("offset"));
            return ret;
        }

        @Override
        public void emitPartitionBatch(TransactionAttempt attempt, TridentCollector collector, GlobalPartitionId partition, Map meta) {
            String instanceId = (String) meta.get("instanceId");
            if (!_config.forceFromStart || instanceId.equals(_topologyInstanceId)) {
                SimpleConsumer consumer = _connections.register(partition);
                long offset = (Long) meta.get("offset");
                long nextOffset = (Long) meta.get("nextOffset");

                LOG.info("re-emitting batch txid: {}, partition: {}, offset: {}, nextOffset: {}", attempt, partition, offset, nextOffset);

                FetchResponse fetchResponse = KafkaUtils.fetch(consumer, _config, partition, offset, new FetchHandler() {
                    @Override
                    public void updateMetrics(long latency) {
                        _kafkaMeanFetchLatencyMetric.update(latency);
                        _kafkaMaxFetchLatencyMetric.update(latency);
                    }
                });

                if (fetchResponse.hasError()) {
                    short code = fetchResponse.errorCode(_config.topic, partition.partition);

                    LOG.error("Error fetching data from the Broker when re-emitting: {}, Reason: {}", partition, ErrorMapping.exceptionFor(code));

                    // TODO: update consumer to new leader and replicas, then return new last meta.
                }

                ByteBufferMessageSet msgSet = fetchResponse.messageSet(_config.topic, partition.partition);

                for (MessageAndOffset msg : msgSet) {
                    if (offset == nextOffset) break;
                    if (offset > nextOffset) {
                        LOG.error("Error when re-emitting batch. overshot the end offset, offset: {}, nextOffset: {}", offset, nextOffset);
                    }
                    TridentUtils.emit(_config, collector, msg.message());
                    offset = msg.offset();
                }
            }
        }

        @Override
        public void close() {
            _connections.clear();
        }

        @Override
        public List<GlobalPartitionId> getOrderedPartitions(List<GlobalPartitionId> partitions) {
            return partitions;
        }

        @Override
        public void refreshPartitions(List<GlobalPartitionId> list) {
            LOG.info("refreshPartitions");
            _connections.clear();
            _kafkaOffsetMetric.refreshPartitions(new HashSet<GlobalPartitionId>(list));
        }
    }


    @Override
    public IPartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new Coordinator(conf, context);
    }

    @Override
    public IPartitionedTridentSpout.Emitter getEmitter(Map conf, TopologyContext context) {
        return new Emitter(conf, context);
    }

    @Override
    public Fields getOutputFields() {
        return _config.scheme.getOutputFields();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}