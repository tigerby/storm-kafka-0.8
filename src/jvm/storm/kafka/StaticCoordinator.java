package storm.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.javaapi.PartitionMetadata;
import storm.kafka.KafkaConfig.StaticHosts;

public class StaticCoordinator implements PartitionCoordinator {

    public static final Logger LOG = LoggerFactory.getLogger(StaticCoordinator.class);

    Map<GlobalPartitionId, PartitionManager> managers = new HashMap<GlobalPartitionId, PartitionManager>();

    public StaticCoordinator(DynamicPartitionConnections connections, Map stormConf,
                             SpoutConfig config, ZkState state, int taskIndex, int totalTasks,
                             String topologyInstanceId) {
        StaticHosts hosts = (StaticHosts) config.hosts;

        List<PartitionMetadata> allPartitionMetadata = new ArrayList<PartitionMetadata>();
        for (int i = 0; i < config.partitions; i++) {
            PartitionMetadata metadata = KafkaUtils.getPartitionMetadata(hosts.hosts, config.topic, i);
            if (metadata != null) {
                allPartitionMetadata.add(metadata);
            }
        }

        for (int i = taskIndex; i < allPartitionMetadata.size(); i += totalTasks) {
            PartitionMetadata metadata = allPartitionMetadata.get(i);
            HostPort hostPort = new HostPort(metadata.leader().host(), metadata.leader().port());

            GlobalPartitionId myPartition = new GlobalPartitionId(hostPort, metadata.partitionId());
            List<HostPort> replicas = KafkaUtils.replicas(metadata);

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
}
