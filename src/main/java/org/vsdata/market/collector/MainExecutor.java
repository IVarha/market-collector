/**
 * Created by igorv on 03.06.15.
 */
package org.vsdata.market.collector;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;


import backtype.storm.tuple.Fields;
import org.vsdata.market.collector.collect.CountBolt;
import org.vsdata.market.collector.collect.HBaseBolt;
import org.vsdata.market.collector.collect.KafkaSpout;
import org.vsdata.market.collector.collect.ParseBolt;


public class MainExecutor {
     private final static String KAFKA_SPOUT = "KAFKA SPOUT";
     private final static String PARSE_BOLT = "PARSE_BOLT";
     private final static String HBASE_BOLT = "HBASE_BOLT";
     private final static String COUNT_BOLT = "COUNT_BOLT";
     public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
          if (args.length == 0) {
               System.out.println("Please provide topic name.");
               return;
          }

          IRichSpout kafkaSpout = new KafkaSpout(args[0]);

          TopologyBuilder builder = new TopologyBuilder();
          builder.setSpout(KAFKA_SPOUT, kafkaSpout).setMaxSpoutPending(200);
          builder.setBolt(PARSE_BOLT, new ParseBolt()).shuffleGrouping(KAFKA_SPOUT);
          builder.setBolt(HBASE_BOLT, new HBaseBolt("/data")).shuffleGrouping("writehbase");
          builder.setBolt(COUNT_BOLT,new CountBolt()).fieldsGrouping(PARSE_BOLT, new Fields("REFER"));

          Config config = new Config();
          if (args != null && args.length > 0) {
               StormSubmitter.submitTopologyWithProgressBar(args[1], config, builder.createTopology());
          } else {
               LocalCluster cluster = new LocalCluster();
               cluster.submitTopology("KafkaTestTopology", config, builder.createTopology());
          }
     }
}
