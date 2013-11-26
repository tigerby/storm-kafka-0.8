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

    List<String> hosts = new ArrayList<String>();
    hosts.add("daisy11:9091");
    hosts.add("daisy11:9092");
    hosts.add("daisy12:9093");
    hosts.add("daisy12:9094");
    KafkaConfig.StaticHosts staticHosts = KafkaConfig.StaticHosts.fromHostString(hosts, 1);
    SpoutConfig spoutConf = new SpoutConfig(
        staticHosts,
//        5,
        "cdrTopic",
        "/storm-kafka-test",
        "cli-storm"
    );

//    spoutConf.zkServers = new ArrayList<String>() {{
//      add("daisy01");
//      add("daisy02");
//      add("daisy03");
//      add("daisy04");
//      add("daisy05");
//    }};
//    spoutConf.zkPort = 2181;

    spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
    spoutConf.forceStartOffsetTime(KafkaConfig.EARLIST_TIME);

    KafkaSpout spout = new KafkaSpout(spoutConf);

    builder.setSpout("kafka-spout", spout, 1);
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
//        KafkaConfig kafkaConf = new KafkaConfig(StaticHosts.fromHostString(hosts, 3), "test");
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
