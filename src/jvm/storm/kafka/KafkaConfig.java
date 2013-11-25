package storm.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.RawMultiScheme;

public class KafkaConfig implements Serializable {
  public static final long EARLIST_TIME = kafka.api.OffsetRequest.EarliestTime();     // -2
  public static final long LATEST_TIME = kafka.api.OffsetRequest.LatestTime();       // -1

  public static interface BrokerHosts extends Serializable {
    HostPort valueOf(String host, int port);
  }

  public static class StaticHosts implements BrokerHosts {

    public static int getNumHosts(BrokerHosts hosts) {
      if (!(hosts instanceof StaticHosts)) {
        throw new RuntimeException("Must use static hosts");
      }
      return ((StaticHosts) hosts).hosts.size();
    }

    public final List<HostPort> hosts;
    public int partitionsPerHost;

    public static StaticHosts fromHostString(List<String> hostStrings, int partitionsPerHost) {
      return new StaticHosts(convertHosts(hostStrings), partitionsPerHost);
    }

    public HostPort valueOf(String host, int port) {
      for(HostPort hp: hosts) {
        if(hp.host.equals(host) && hp.port == port) {
          return hp;
        }
      }
      throw new RuntimeException("invalid host/port: " + host + ", " + port);
    }

    public StaticHosts(List<HostPort> hosts, int partitionsPerHost) {
      this.hosts = hosts;
      this.partitionsPerHost = partitionsPerHost;
    }

  }

  public static class ZkHosts implements BrokerHosts {

    public String brokerZkStr = null;
    public String brokerZkPath = null; // e.g., /kafka/brokers
    public int refreshFreqSecs = 60;

    public ZkHosts(String brokerZkStr, String brokerZkPath) {
      this.brokerZkStr = brokerZkStr;
      this.brokerZkPath = brokerZkPath;
    }

    @Override
    public HostPort valueOf(String host, int port) {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
  }


  public BrokerHosts hosts;
  public int fetchSizeBytes = 1024 * 1024;
  public int socketTimeoutMs = 10000;
  public int bufferSizeBytes = 1024 * 1024;
  public MultiScheme scheme = new RawMultiScheme();
  public String topic;
  public long startOffsetTime = EARLIST_TIME;
  public boolean forceFromStart = false;

  public KafkaConfig(BrokerHosts hosts, String topic) {
    this.hosts = hosts;
    this.topic = topic;
  }


  public void forceStartOffsetTime(long millis) {
    startOffsetTime = millis;
    forceFromStart = true;
  }

  public static List<HostPort> convertHosts(List<String> hosts) {
    List<HostPort> ret = new ArrayList<HostPort>();
    for (String s : hosts) {
      HostPort hp;
      String[] spec = s.split(":");
      if (spec.length == 1) {
        hp = new HostPort(spec[0]);
      } else if (spec.length == 2) {
        hp = new HostPort(spec[0], Integer.parseInt(spec[1]));
      } else {
        throw new IllegalArgumentException("Invalid host specification: " + s);
      }
      ret.add(hp);
    }
    return ret;
  }
}
