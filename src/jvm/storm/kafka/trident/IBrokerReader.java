package storm.kafka.trident;

import storm.kafka.GlobalPartitionId;
import storm.kafka.HostPort;

import java.util.List;
import java.util.Map;


public interface IBrokerReader {    
    /**
     * Map of host to [port, numPartitions]
     */
    List<GlobalPartitionId> getCurrentBrokers();
    void close();
}
