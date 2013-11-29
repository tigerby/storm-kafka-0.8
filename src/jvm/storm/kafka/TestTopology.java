package storm.kafka;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class TestTopology {

  public static class PrinterBolt extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String msg = tuple.getStringByField("str");
      System.out.println(msg);
    }

  }

  public static void main(String[] args) {
    TopologyBuilder builder = new TopologyBuilder();

    KafkaConfig.StaticHosts staticHosts =
        KafkaConfig.StaticHosts
            .newInstance("daisy11:9091,daisy11:9092,daisy12:9093,daisy12:9094");

    SpoutConfig spoutConf =
        new SpoutConfig.Builder(staticHosts, "ips", 5, "/storm-kafka-test", "cli-storm")
            .zkServers(SpoutConfig.fromHostString("daisy01,daisy02,daisy03,daisy04,daisy05"))
            .zkPort(2181)
            .scheme(new SchemeAsMultiScheme(new StringScheme()))
            .startOffsetTime(KafkaConfig.EARLIEST_TIME)
            .build();

    KafkaSpout spout = new KafkaSpout(spoutConf);

    builder.setSpout("kafka-spout", spout, 5);
    builder.setBolt("print-bolt", new PrinterBolt(), 1)
        .shuffleGrouping("kafka-spout");

    Config conf = new Config();
    //conf.setDebug(true);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("kafka-test", conf, builder.createTopology());

    Utils.sleep(600000);
  }

//    public static void main(String [] args) throws Exception {
//        List<String> hosts = new ArrayList<String>();
//        hosts.add("localhost");
//        KafkaConfig kafkaConf = new KafkaConfig(StaticHosts.newInstance(hosts, 3), "test");
//        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
//        LocalCluster cluster = new LocalCluster();
//        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("id", "spout",
//                new OpaqueTransactionalKafkaSpout(kafkaConf), 1);
//        builder.setBolt("printer", new PrinterBolt())
//                .shuffleGrouping("spout");
//        Config config = new Config();
//
//        cluster.submitTopology("kafka-test", config, builder.buildTopology());
//
//        Thread.sleep(600000);
//    }

}
