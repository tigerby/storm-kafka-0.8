package storm.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.RawMultiScheme;


public class SpoutConfig extends KafkaConfig implements Serializable {

  public List<String> zkServers = null;
  public Integer zkPort = null;
  public String zkRoot = null;
  public String id = null;
  public long stateUpdateIntervalMs = 2000;

  public static class Builder {
    private final BrokerHosts hosts;
    private final String topic;
    private final int partitions;
    private final String zkRoot;
    private final String id;

    private List<String> zkServers;
    private Integer zkPort;
    private long stateUpdateIntervalMs = 2000;
    private MultiScheme scheme = new RawMultiScheme();
    private long startOffsetTime = EARLIEST_TIME;
    private long forceStartOffsetTime;

    private boolean doesStartTimeForced;


    public Builder(BrokerHosts hosts, String topic, int partitions, String zkRoot, String id) {
      this.hosts = hosts;
      this.topic = topic;
      this.partitions = partitions;
      this.zkRoot = zkRoot;
      this.id = id;
    }

    public Builder zkServers(List<String> zkServers) {
      this.zkServers = zkServers;
      return this;
    }

    public Builder zkPort(Integer zkPort) {
      this.zkPort = zkPort;
      return this;
    }

    public Builder stateUpdateIntervalMs(long stateUpdateIntervalMs) {
      this.stateUpdateIntervalMs = stateUpdateIntervalMs;
      return this;
    }

    public Builder scheme(MultiScheme scheme) {
      this.scheme = scheme;
      return this;
    }

    public Builder startOffsetTime(long startOffsetTime) {
      this.startOffsetTime = startOffsetTime;
      this.doesStartTimeForced = true;
      return this;
    }

    public SpoutConfig build() {
      return new SpoutConfig(this);
    }

  }

  private SpoutConfig(Builder builder) {
    super(builder.hosts, builder.topic, builder.partitions);
    zkRoot = builder.zkRoot;
    id = builder.id;
    zkServers = builder.zkServers;
    zkPort = builder.zkPort;
    stateUpdateIntervalMs = builder.stateUpdateIntervalMs;
    scheme = builder.scheme;

    if(builder.doesStartTimeForced) {
      forceStartOffsetTime(builder.startOffsetTime);
    }
  }


  public static List<String> fromHostString(String hosts) {
    List<String> list = new ArrayList<String>();
    String[] split = hosts.split(",");
    for(String host: split) {
      list.add(host);
    }
    return list;
  }
}
