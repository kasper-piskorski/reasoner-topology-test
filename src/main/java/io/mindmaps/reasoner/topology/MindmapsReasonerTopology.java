package io.mindmaps.reasoner.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import storm.kafka.*;

import io.mindmaps.reasoner.topology.bolt.*;

public class MindmapsReasonerTopology{

    static String zkHost = "127.0.0.1";
    static String zkPort = "2181";
    static String kafkaPort = "9092";
    static String topic = "storm-topic";


    public static StormTopology buildTopology() {
        BrokerHosts brokerHosts = new ZkHosts(zkHost + ":" + zkPort);
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topic, "", "storm-test");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("record", new KafkaSpout(kafkaConfig), 1);
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping("record");
        return builder.createTopology();
    }

    public static void main(String[] argv) throws Exception {

        int workers = 1;

        Config config = new Config();

        config.setNumWorkers(workers);

        StormSubmitter.submitTopologyWithProgressBar(argv[0], config, buildTopology());

    }

}
