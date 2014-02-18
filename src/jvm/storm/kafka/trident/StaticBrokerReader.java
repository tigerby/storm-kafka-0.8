package storm.kafka.trident;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import storm.kafka.GlobalPartitionId;
import storm.kafka.HostPort;
import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.PartitionManager;


public class StaticBrokerReader implements IBrokerReader {

    List<GlobalPartitionId> brokers = new ArrayList<GlobalPartitionId>();

    public StaticBrokerReader(TridentKafkaConfig config) {
        List<PartitionMetadata> allPartitionMetadata = new ArrayList<PartitionMetadata>();
        for (int i = 0; i < config.partitions; i++) {
            PartitionMetadata metadata = KafkaUtils.findLeader(((StaticHosts)config.hosts).hosts, config.topic, i);
            if (metadata != null) {
                Broker leader = metadata.leader();
                brokers.add(new GlobalPartitionId(new HostPort(leader.host(), leader.port()), i));
            }
        }

        if(allPartitionMetadata == null)    throw new RuntimeException("there no partition metadata, " + config.topic);

    }

    @Override
    public List<GlobalPartitionId> getCurrentBrokers() {
        return brokers;
    }

    @Override
    public void close() {
    }
}
