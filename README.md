## storm-kafka-0.8

this version of storm-kafka provides a regular spout implementation and a TransactionalSpout implementation for Apache Kafka 0.8. and upper.

It's ported from nathanmarz/storm-contrib/storm-kafka


## Using KafkaSpout

`KafkaSpout` is a regular spout implementation that reads from a Kafka cluster. The basic usage is like this:

```java


// list of Kafka brokers
KafkaConfig.StaticHosts staticHosts = KafkaConfig.StaticHosts
        .newInstance("daisy11:9091,daisy11:9092,daisy12:9093,daisy12:9094");

SpoutConfig spoutConfig =
    new SpoutConfig.Builder(staticHosts, "ips", 5, "/storm-kafka-test", "cli-storm")
        .zkServers(SpoutConfig.fromHostString("daisy01,daisy02,daisy03,daisy04,daisy05"))
        .zkPort(2181)
        .scheme(new SchemeAsMultiScheme(new StringScheme()))
        .startOffsetTime(KafkaConfig.EARLIEST_TIME)
        .build();

KafkaSpout spout = new KafkaSpout(spoutConfig);
```

Currently the spout is parameterized with a static list of brokers, topic to read from, fixed number of partitions, the root path in Zookeeper for the spout to store the consumer offsets and, an id for this consumer for storing the consumer offsets in Zookeeper.

By default, the offsets will be stored in the same Zookeeper cluster that Storm uses. You can override this via your spout config using `.zkServers` and `.zkPort` in Builder.

Another very useful config in the spout is the ability to force the spout to rewind to a previous offset. You do `startOffsetTime` on the spout config using `startOffsetTime`.


It will choose the latest offset written around that timestamp to start consuming. You can force the spout to always start from the latest offset by passing in `KafkaConfig.LASTEST_TIME`, and you can force it to start from the earliest offset by passing in `KafkaConfig.EARLIEST_TIME`.

You can do above things just running `TestTopology.main()`.

Finally, `SpoutConfig` has options for adjusting how the spout fetches messages from Kafka (buffer sizes, amount of messages to fetch at a time, timeouts, etc.).




