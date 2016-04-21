package io.mindmaps.reasoner.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.IMetricsContext;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


/*
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
*/
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;


import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

/*
        //Produce some messages
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafkaHost + ":" + kafkaPort);
        producerProps.put("acks", "all");
        producerProps.put("retries", 0);
        producerProps.put("batch.size", 16384);
        producerProps.put("linger.ms", 1);
        producerProps.put("buffer.memory", 33554432);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(producerProps);

        //ProducerRecord(String topic, int partition, KEY, VALUE);
        for(int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("test-topic2", Integer.toString(i), Integer.toString(i)));
            System.out.printf("Record %d produced \n", i);
        }
*/
        
        /*
        System.out.printf("Preparing to consume messages...");

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafkaHost + ":" + kafkaPort);
        consumerProps.put("group.id", "test");
        //read from beginning
        consumerProps.put("auto.offset.reset","earliest");
        //consumerProps.put("enable.auto.commit", "true");
        //consumerProps.put("auto.commit.interval.ms", "1000");
        //consumerProps.put("session.timeout.ms", "30000");
        //consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProps);

        List<PartitionInfo> pInfo = consumer.partitionsFor(topic);
        System.out.print("Available partitions: \n");
        for( PartitionInfo partition : pInfo) {
            System.out.print("topic : " + topic + " partition : " + partition.partition() +"\n");
        }
        TopicPartition partition0 = new TopicPartition(topic, 0);

        //subscribe to a given topic
        //consumer.subscribe(Arrays.asList(topic));

        //subscribe to a specific partition of a topic
        consumer.assign(Arrays.asList(partition0));
        consumer.seekToBeginning(partition0);

        System.out.printf("Consumer position: %d", consumer.position(partition0));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
        }

        */

        int workers = 1;
        int parallelism = 1;

        Config config = new Config();

        config.setNumWorkers(workers);

        /*
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(null, config, buildTopology());
        */

        StormSubmitter.submitTopologyWithProgressBar(argv[0], config, buildTopology());
        /*
        System.out.println("=================================================================================================");
        System.out.println("|||||  Storm Topology submitted with " + workers + " worker(s) and " + parallelism + " parallelism level for every worker   |||||");
        System.out.println("=================================================================================================");
        */
    }

}
