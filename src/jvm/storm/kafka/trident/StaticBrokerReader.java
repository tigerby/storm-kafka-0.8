package storm.kafka.trident;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.kafka.HostPort;
import storm.kafka.KafkaConfig.StaticHosts;


public class StaticBrokerReader implements IBrokerReader {

    Map<String, List> brokers = new HashMap();

    public StaticBrokerReader(TridentKafkaConfig config) {
        StaticHosts hosts = (StaticHosts) config.hosts;
        for (HostPort hp : hosts.hosts) {
            List info = new ArrayList();
            info.add((long) hp.port);
            info.add((long) config.partitions);
            brokers.put(hp.host, info);
        }
    }

    @Override
    public Map<String, List> getCurrentBrokers() {
        return brokers;
    }

    @Override
    public void close() {
    }
}
