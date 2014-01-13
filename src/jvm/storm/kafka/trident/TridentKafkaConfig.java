package storm.kafka.trident;

import storm.kafka.KafkaConfig;


public class TridentKafkaConfig extends KafkaConfig {
    public TridentKafkaConfig(BrokerHosts hosts, String topic, int partitions) {
        super(hosts, topic, partitions);
    }
    
    public IBatchCoordinator coordinator = new DefaultCoordinator();
}
