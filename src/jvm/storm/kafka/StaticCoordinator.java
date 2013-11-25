package storm.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import storm.kafka.KafkaConfig.StaticHosts;

// TODO: find new leader when state is obsolete and update PartitionManager.
public class StaticCoordinator implements PartitionCoordinator {

  public static final Logger LOG = LoggerFactory.getLogger(StaticCoordinator.class);

  Map<GlobalPartitionId, PartitionManager> managers =
      new HashMap<GlobalPartitionId, PartitionManager>();
  List<String> replicaBrokers = new ArrayList<String>();

  public StaticCoordinator(DynamicPartitionConnections connections, Map stormConf,
                           SpoutConfig config, ZkState state, int taskIndex, int totalTasks,
                           String topologyInstanceId) {
//    StaticHosts hosts = (StaticHosts) config.hosts;
//    List<GlobalPartitionId> allPartitionIds = new ArrayList();
//    for (HostPort h : hosts.hosts) {
//      for (int i = 0; i < hosts.partitionsPerHost; i++) {
//        allPartitionIds.add(new GlobalPartitionId(h, i));
//      }
//    }
//    for (int i = taskIndex; i < allPartitionIds.size(); i += totalTasks) {
//      GlobalPartitionId myPartition = allPartitionIds.get(i);
//      managers.put(myPartition,
//                    new PartitionManager(connections, topologyInstanceId, state, stormConf, config,
//                                         myPartition));
//
//    }

    StaticHosts hosts = (StaticHosts) config.hosts;

    for (int i = 0; i < hosts.partitionsPerHost; i++) {
      HostPort hp = hosts.hosts.get(0);
      String seed = hp.host;

      PartitionMetadata metadata = findLeader(seed, hp.port, config.topic, i);
      HostPort hostPort = hosts.valueOf(metadata.leader().host(), metadata.leader().port());

      GlobalPartitionId myPartition = new GlobalPartitionId(hostPort, metadata.partitionId());
      managers.put(new GlobalPartitionId(hostPort, metadata.partitionId()),
                   new PartitionManager(connections, topologyInstanceId, state, stormConf, config,
                                        myPartition));

      replicaBrokers = replicas(metadata);
    }

  }

  @Override
  public List<PartitionManager> getMyManagedPartitions() {
    return new ArrayList(managers.values());
  }

  public PartitionManager getManager(GlobalPartitionId id) {
    return managers.get(id);
  }

  private static PartitionMetadata findLeader(String seedBroker, int port,
                                              String topic, int partition) {
    ArrayList<String> brokerList = new ArrayList<String>();
    brokerList.add(seedBroker);

    return findLeader(brokerList, port, topic, partition);
  }

  private static PartitionMetadata findLeader(List<String> seedBrokers, int port,
                                              String topic, int partition) {
    PartitionMetadata returnMetaData = null;
    for (String seed : seedBrokers) {
      SimpleConsumer consumer = null;
      try {
        consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "leaderLookup");
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
        LOG.error("Error communicating with Broker [{}] to find Leader for [{}, {}] Reason: ", seed,
                  topic, partition, e);
      } finally {
        if (consumer != null) {
          consumer.close();
        }
      }
    }

    return returnMetaData;
  }

  public static List<String> replicas(PartitionMetadata metadata) {
    List<String> replicaBrokers = new ArrayList<String>();
    for (kafka.cluster.Broker replica : metadata.replicas()) {
      replicaBrokers.add(replica.host());
    }
    return replicaBrokers;
  }

  private static String findNewLeader(List<String> m_replicaBrokers, String a_oldLeader,
                                      String a_topic, int a_partition, int a_port)
      throws Exception {
    for (int i = 0; i < 3; i++) {
      boolean goToSleep = false;
      PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition);
      if (metadata == null) {
        goToSleep = true;
      } else if (metadata.leader() == null) {
        goToSleep = true;
      } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
        // first time through if the leader hasn't changed give ZooKeeper a second to recover
        // second time, assume the broker did recover before failover, or it was a non-Broker issue
        //
        goToSleep = true;
      } else {
        return metadata.leader().host();
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


}
