package storm.kafka.trident;

import java.util.List;
import java.util.Map;
import storm.kafka.DynamicBrokersReader;
import storm.kafka.GlobalPartitionId;
import storm.kafka.HostPort;
import storm.kafka.KafkaConfig.ZkHosts;


public class ZkBrokerReader implements IBrokerReader {

    List<GlobalPartitionId> cachedBrokers;
    DynamicBrokersReader reader;
    long lastRefreshTimeMs;
    long refreshMillis;
    
    public ZkBrokerReader(Map conf, String topic, ZkHosts hosts) {
//        reader = new DynamicBrokersReader(conf, hosts.brokerZkStr, hosts.brokerZkPath, topic);
//        cachedBrokers = reader.getBrokerInfo();
//        lastRefreshTimeMs = System.currentTimeMillis();
//        refreshMillis = hosts.refreshFreqSecs * 1000L;
    }
    
    @Override
    public List<GlobalPartitionId> getCurrentBrokers() {
//        long currTime = System.currentTimeMillis();
//        if(currTime > lastRefreshTimeMs + refreshMillis) {
//            cachedBrokers = reader.getBrokerInfo();
//            lastRefreshTimeMs = currTime;
//        }
        return cachedBrokers;
    }

    @Override
    public void close() {
        reader.close();
    }    
}
