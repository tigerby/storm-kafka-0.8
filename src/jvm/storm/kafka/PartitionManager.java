package storm.kafka;

import backtype.storm.Config;
import backtype.storm.metric.api.*;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;

import com.google.common.collect.ImmutableMap;

import java.util.*;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.KafkaSpout.EmitState;
import storm.kafka.KafkaSpout.MessageAndRealOffset;
import storm.kafka.trident.MaxMetric;

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
  GlobalPartitionId _partition;
  SpoutConfig _spoutConfig;
  String _topologyInstanceId;
  SimpleConsumer _consumer;
  DynamicPartitionConnections _connections;
  ZkState _state;
  Map _stormConf;


  public PartitionManager(DynamicPartitionConnections connections, String topologyInstanceId,
                          ZkState state, Map stormConf, SpoutConfig spoutConfig,
                          GlobalPartitionId id) {
    _partition = id;
    _connections = connections;
    _spoutConfig = spoutConfig;
    _topologyInstanceId = topologyInstanceId;
    _consumer = connections.register(id.host, id.partition);
    _state = state;
    _stormConf = stormConf;

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
      _committedTo =
          getLastOffset(_consumer, spoutConfig.topic, id.partition, spoutConfig.startOffsetTime,
                        "cli-" + spoutConfig.topic + "-" + id.partition);
      LOG.info("Using startOffsetTime to choose last commit offset.");
    } else if (jsonTopologyId == null || jsonOffset == null) { // failed to parse JSON?
      _committedTo =
          getLastOffset(_consumer, spoutConfig.topic, id.partition,
                        kafka.api.OffsetRequest.LatestTime(),
                        "cli-" + spoutConfig.topic + "-" + id.partition);
      LOG.info("Setting last commit offset to HEAD.");
    } else {
      _committedTo = jsonOffset;
      LOG.info("Read last commit offset from zookeeper: " + _committedTo);
    }

    LOG.info("Starting Kafka {}:{} from offset ", _consumer.host(), id.partition, _committedTo);
    _emittedToOffset = _committedTo;

    _fetchAPILatencyMax = new CombinedMetric(new MaxMetric());
    _fetchAPILatencyMean = new ReducedMetric(new MeanReducer());
    _fetchAPICallCount = new CountMetric();
    _fetchAPIMessageCount = new CountMetric();
  }

  public long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                            long whichTime, String clientName) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo>
        requestInfo =
        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
    kafka.javaapi.OffsetRequest
        request =
        new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
                                        clientName);
    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      System.out.println("Error fetching data Offset Data the Broker. Reason: " + response
          .errorCode(topic, partition));
      return 0;
    }
    long[] offsets = response.offsets(topic, partition);
    return offsets[0];
  }

  public Map getMetricsDataMap() {
    Map ret = new HashMap();
    ret.put(_partition + "/fetchAPILatencyMax", _fetchAPILatencyMax.getValueAndReset());
    ret.put(_partition + "/fetchAPILatencyMean", _fetchAPILatencyMean.getValueAndReset());
    ret.put(_partition + "/fetchAPICallCount", _fetchAPICallCount.getValueAndReset());
    ret.put(_partition + "/fetchAPIMessageCount", _fetchAPIMessageCount.getValueAndReset());
    return ret;
  }

  //returns false if it's reached the end of current batch
  public EmitState next(SpoutOutputCollector collector) {
    if (_waitingToEmit.isEmpty()) {
      fill();
    }
    while (true) {
      MessageAndRealOffset toEmit = _waitingToEmit.pollFirst();
      if (toEmit == null) {
        return EmitState.NO_EMITTED;
      }
      Iterable<List<Object>>
          tups =
          _spoutConfig.scheme.deserialize(Utils.toByteArray(toEmit.msg.payload()));
      if (tups != null) {
        for (List<Object> tup : tups) {
          collector.emit(tup, new KafkaMessageId(_partition, toEmit.offset));
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
    LOG.info("Fetching from Kafka: {}:{} from offset {}", _consumer.host(), _partition.partition,
             _emittedToOffset);
    long start = System.nanoTime();

    FetchRequest req = new FetchRequestBuilder()
        .clientId("cli-" + _spoutConfig.topic + "-" + _partition.partition)
        .addFetch(_spoutConfig.topic, _partition.partition, _emittedToOffset,
                  _spoutConfig.fetchSizeBytes)
        .build();

    FetchResponse fetchResponse = _consumer.fetch(req);

    ByteBufferMessageSet messageAndOffsets =
        fetchResponse.messageSet(_spoutConfig.topic, _partition.partition);
    int numMessages = messageAndOffsets.sizeInBytes();
    long end = System.nanoTime();
    long millis = (end - start) / 1000000;
    _fetchAPILatencyMax.update(millis);
    _fetchAPILatencyMean.update(millis);
    _fetchAPICallCount.incr();
    _fetchAPIMessageCount.incrBy(numMessages);

    if (numMessages > 0) {
      LOG.info("Fetched " + numMessages + " byte messages from Kafka: " + _consumer.host() + ":"
               + _partition.partition);
    }
    for (MessageAndOffset messageAndOffset : messageAndOffsets) {
      long currentOffset = messageAndOffset.offset();
      // TODO: shouldn't check currentOffset and read offset?
//      if (currentOffset < readOffset) {
//        System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
//        continue;
//      }
//      readOffset = messageAndOffset.nextOffset();

      _pending.add(_emittedToOffset);
      _waitingToEmit.add(new MessageAndRealOffset(messageAndOffset.message(), _emittedToOffset));
      _emittedToOffset = messageAndOffset.offset();
    }
    if (numMessages > 0) {
      LOG.info("Added " + numMessages + " byte messages from Kafka: " + _consumer.host() + ":"
               + _partition.partition + " to internal buffers");
    }
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
    LOG.info("Committing offset for {}", _partition);
    long committedTo;
    if (_pending.isEmpty()) {
      committedTo = _emittedToOffset;
    } else {
      committedTo = _pending.first();
    }
    if (committedTo != _committedTo) {
      LOG.info("Writing committed offset to ZK: {}", committedTo);

      Map<Object, Object> data = ImmutableMap.builder()
          .put("topology", ImmutableMap.of("id", _topologyInstanceId,
                                           "name", _stormConf.get(Config.TOPOLOGY_NAME)))
          .put("offset", committedTo)
          .put("partition", _partition.partition)
          .put("broker", ImmutableMap.of("host", _partition.host.host,
                                         "port", _partition.host.port))
          .put("topic", _spoutConfig.topic).build();
      _state.writeJSON(committedPath(), data);

      LOG.info("Wrote committed offset to ZK: {}", committedTo);
      _committedTo = committedTo;
    }
    LOG.info("Committed offset {} for {}", committedTo, _partition);
  }

  private String committedPath() {
    return _spoutConfig.zkRoot + "/" + _spoutConfig.id + "/" + _partition;
  }

  public long queryPartitionOffsetLatestTime() {
    return getLastOffset(_consumer, _spoutConfig.topic, _partition.partition,
                         OffsetRequest.LatestTime(),
                         "cli-" + _spoutConfig.topic + "-" + _partition.partition);
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
    return _partition;
  }

  public void close() {
    _connections.unregister(_partition.host, _partition.partition);
  }
}
