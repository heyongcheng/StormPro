package com.he.strom.noack;

import java.util.Arrays;
import java.util.Map;
import java.util.Stack;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class SentenceSpout extends BaseRichSpout{
	
	private SpoutOutputCollector collector ;

	private static String[] sentences = {
			"my dog has fleas",
			"i like cold beverages",
			"the dog ate my homework",
			"don't have a cow man",
			"i don't think i like fleas"};
	
	private static Stack<String> stack;
	
	static{
		stack = new Stack<String>();
		stack.addAll(Arrays.asList(sentences));
	}
	
	private int index = 0;
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void nextTuple() {
		/*if(index < sentences.length){
			this.collector.emit(new Values(sentences[index]));			
			index ++;
		}*/
		/*if(index >= sentences.length){
			index = 0;
		}*/
		if(!stack.isEmpty()){
			this.collector.emit(new Values(stack.pop()));			
		}
		Utils.sleep(1);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
