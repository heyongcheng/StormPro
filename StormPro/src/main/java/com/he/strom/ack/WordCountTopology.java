package com.he.strom.ack;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class WordCountTopology {
	
	private static final String SENTSENCE_SPOUT_ID = "sentence-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String REPORT_BOLT_ID = "report-bolt";
	private static final String TOPOLOGY_NAME = "world-count-topology";
	
	public static void main(String[] args) {
		SentenceSpout spout = new SentenceSpout();
		SplitSentenceBolt splitBolt = new SplitSentenceBolt();
		WordCountBolt countBolt = new WordCountBolt();
		ReportBolt reportBolt = new ReportBolt();
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SENTSENCE_SPOUT_ID, spout,4);
		builder.setBolt(SPLIT_BOLT_ID, splitBolt,2).setNumTasks(4).shuffleGrouping(SENTSENCE_SPOUT_ID);
		builder.setBolt(COUNT_BOLT_ID, countBolt,4).fieldsGrouping(SPLIT_BOLT_ID,new Fields("word"));
		builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);
		
		Config config = new Config();
		config.setNumWorkers(2);
		
		/** Local 模式  **/
		/*LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();*/
		
		try {
			StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
