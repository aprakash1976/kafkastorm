package com.clarity.storm.kafkastorm;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


public class ApacheLogTopology {
	
	public static final Logger LOG = LoggerFactory.getLogger(ApacheLogTopology.class);

    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            LOG.info(tuple.toString());
        }

    }

    private final BrokerHosts brokerHosts;

    public ApacheLogTopology(String kafkaZookeeper) {
        brokerHosts = new ZkHosts(kafkaZookeeper);
    }

    public StormTopology buildTopology() {
    	LOG.info("***********calling buildTopology**********");
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "clarity.apachelog", "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("words", new KafkaSpout(kafkaConfig), 10);
        builder.setBolt("parse", new ApacheLogParserBolt("parse", true)).shuffleGrouping("words");
        TupleTableConfig config = new TupleTableConfig("apache_access_log", "row_id");
        config.setBatch(false);
        config.addColumn("common", "hostname");
        config.addColumn("common", "remoteuser");
        config.addColumn("common", "eventtimestamp");
        config.addColumn("http", "requestmethod");
        config.addColumn("http", "requeststatus");
        config.addColumn("http", "responsebytes");
        config.addColumn("common", "remotehost");
        config.addColumn("misc", "agent");
        builder.setBolt("hbase", new HBaseBolt("hbase", config, true)).shuffleGrouping("parse");
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {

        String kafkaZk = args[0];
        ApacheLogTopology kafkaSpoutTestTopology = new ApacheLogTopology(kafkaZk);
        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 20000);

        StormTopology stormTopology = kafkaSpoutTestTopology.buildTopology();
        if (args != null && args.length > 1) {
            String name = args[1];
            String dockerIp = args[2];
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(5);
            config.put(Config.NIMBUS_HOST, dockerIp);
            config.put(Config.NIMBUS_THRIFT_PORT, 6627);
            config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(dockerIp));
            StormSubmitter.submitTopology(name, config, stormTopology);
        } else {
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka", config, stormTopology);
        }
    }

}
