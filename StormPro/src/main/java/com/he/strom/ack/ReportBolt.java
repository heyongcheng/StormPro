package com.he.strom.ack;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class ReportBolt extends BaseRichBolt{

	private OutputCollector collector;
	
	private Map<String,Long> counts;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.counts = new HashMap<String,Long>();
	}

	public void execute(Tuple input) {
		String world = input.getStringByField("word");
		Long count = input.getLongByField("count");
		this.counts.put(world, count);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
	@Override
	public void cleanup(){
		System.out.println("******FINAL COUNT******");
		Iterator<Entry<String, Long>> iterator = counts.entrySet().iterator();
		while(iterator.hasNext()){
			Entry<String, Long> entry = iterator.next();
			System.out.println(entry.getKey() + ":" + entry.getValue());
		}
		System.out.println("******    END    ******");
	}

}
