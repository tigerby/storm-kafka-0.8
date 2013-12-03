package storm.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import storm.kafka.KafkaConfig.StaticHosts;

public class StaticCoordinator implements PartitionCoordinator {

  public static final Logger LOG = LoggerFactory.getLogger(StaticCoordinator.class);

  Map<GlobalPartitionId, PartitionManager> managers =
      new HashMap<GlobalPartitionId, PartitionManager>();

  public StaticCoordinator(DynamicPartitionConnections connections, Map stormConf,
                           SpoutConfig config, ZkState state, int taskIndex, int totalTasks,
                           String topologyInstanceId) {
    StaticHosts hosts = (StaticHosts) config.hosts;

    List<PartitionMetadata> allPartitionMetadata = new ArrayList<PartitionMetadata>();
    for (int i = 0; i < config.partitions; i++) {
      PartitionMetadata metadata = findLeader(hosts.hosts, config.topic, i);
      if(metadata != null) {
        allPartitionMetadata.add(metadata);
      }
    }

    for(int i=taskIndex; i<allPartitionMetadata.size(); i+=totalTasks) {
      PartitionMetadata metadata = allPartitionMetadata.get(i);
      HostPort hostPort = hosts.valueOf(metadata.leader().host(), metadata.leader().port());

      List<HostPort> replicas = new ArrayList<HostPort>();
      for(Broker broker: metadata.replicas()) {
        HostPort replica = hosts.valueOf(broker.host(), broker.port());
        replicas.add(replica);
      }

      GlobalPartitionId myPartition = new GlobalPartitionId(hostPort, metadata.partitionId());
      managers.put(new GlobalPartitionId(hostPort, metadata.partitionId()),
                   new PartitionManager(connections, topologyInstanceId, state, stormConf, config,
                                        myPartition, replicas));

    }
  }

  @Override
  public List<PartitionManager> getMyManagedPartitions() {
    return new ArrayList<PartitionManager>(managers.values());
  }

  public PartitionManager getManager(GlobalPartitionId id) {
    return managers.get(id);
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

  public static HostPort findNewLeader(List<HostPort> replicaBrokers, HostPort oldLeader, String topic, int partition) {
    // TODO: property retry count.
    for (int i = 0; i < 10; i++) {
      LOG.info("trying to find new leader: {} times", i + 1);

      boolean goToSleep = false;
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


}
