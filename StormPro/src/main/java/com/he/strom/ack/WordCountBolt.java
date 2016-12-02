package com.he.strom.ack;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt{

	private OutputCollector collector;
	
	private Map<String,Long> counts;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.counts = new HashMap<String,Long>();
	}

	public void execute(Tuple input) {
		String world = input.getStringByField("word");
		Long count = this.counts.get(world);
		if(count == null){
			count = 0L;
		}
		count ++;
		this.counts.put(world, count);
		this.collector.emit(new Values(world,count));
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count"));
	}

}
