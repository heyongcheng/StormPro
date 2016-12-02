package com.he.strom.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

public class OutbreakDetectionTopology {
	
	public static StormTopology buildTopology(){
		
		TridentTopology topology = new TridentTopology();
		
		DiagnosisEventSpout spout = new DiagnosisEventSpout();
		
		Stream stream = topology.newStream("event", spout);
		//过滤事件
		stream.each(new Fields("event"), new DiseaseFilter())
		//
		.each(new Fields("event"), new CityAssignment(),new Fields("city"))
		//
		.each(new Fields("event","city"), new HourAssignment(), new Fields("hour","cityDiseaseHour"))
		//
		.groupBy(new Fields("cityDiseaseHour"))
		//
		.persistentAggregate(new OutbreakTrendFactory(), new Count(), new Fields("count"))
		.newValuesStream()
		//
		.each(new Fields("cityDiseaseHour","count"), new OutbreakDetector(),new Fields("alert"))
		//
		.each(new Fields("alert"),new DispatchAlert(),new Fields());
		
		return topology.build();
	}
	
	public static void main(String[] args) {
		try {
			Config config = new Config();
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("cdc", config, buildTopology());
			Thread.sleep(200000);
			localCluster.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
