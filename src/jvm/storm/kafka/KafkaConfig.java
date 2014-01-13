package storm.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.RawMultiScheme;

public class KafkaConfig implements Serializable {
  public static final long EARLIEST_TIME = kafka.api.OffsetRequest.EarliestTime();     // -2
  public static final long LATEST_TIME = kafka.api.OffsetRequest.LatestTime();        // -1

  public static interface BrokerHosts extends Serializable {
    HostPort valueOf(String host, int port);
  }

  public static class StaticHosts implements BrokerHosts {

    public static final Logger LOG = LoggerFactory.getLogger(StaticHosts.class);

    public static int getNumHosts(BrokerHosts hosts) {
      if (!(hosts instanceof StaticHosts)) {
        throw new RuntimeException("Must use static hosts");
      }
      return ((StaticHosts) hosts).hosts.size();
    }

    public final List<HostPort> hosts;

    public static StaticHosts newInstance(List<String> hostList) {
      return new StaticHosts(convertHosts(hostList));
    }

    public static StaticHosts newInstance(String hostStrings) {
      return new StaticHosts(convertHosts(hostStrings));
    }

    public HostPort valueOf(String host, int port) {
      for(HostPort hp: hosts) {
        if(hp.host.equals(host) && hp.port == port) {
          return hp;
        }
      }

      LOG.info("add new broker, {}:{}", host, port);
      HostPort newOne = new HostPort(host, port);
      hosts.add(newOne);
      return newOne;
    }

    public StaticHosts(List<HostPort> hosts) {
      this.hosts = hosts;
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
  // TODO: property
  public int fetchSizeBytes = 1024 * 1024;
  public int socketTimeoutMs = 10000;
  public int bufferSizeBytes = 1024 * 1024;
  public MultiScheme scheme = new RawMultiScheme();
  public String topic;
  public int partitions;
  public long startOffsetTime = EARLIEST_TIME;
  public boolean forceFromStart = false;

  public KafkaConfig(BrokerHosts hosts, String topic, int partitions) {
    this.hosts = hosts;
    this.topic = topic;
    this.partitions = partitions;
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

  public static List<HostPort> convertHosts(String hosts) {
    List<HostPort> ret = new ArrayList<HostPort>();
    String[] split = hosts.split(",");
    for (String s : split) {
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
