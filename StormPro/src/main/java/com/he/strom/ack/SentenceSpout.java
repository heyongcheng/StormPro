package com.he.strom.ack;

import java.util.Arrays;
import java.util.Map;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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
	
	private static Map<UUID,Values> pending;
	
	static{
		stack = new Stack<String>();
		stack.addAll(Arrays.asList(sentences));
	}
	
	private int index = 0;
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.pending = new ConcurrentHashMap<UUID, Values>();
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
			Values values = new Values(stack.pop());
			UUID uuid = UUID.randomUUID();
			pending.put(uuid, values);
			this.collector.emit(values,uuid);			
		}
		Utils.sleep(1);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

	@Override
	public void ack(Object msgId) {
		super.ack(msgId);
		this.pending.remove(msgId);
	}
	
	@Override
	public void fail(Object msgId) {
		super.fail(msgId);
		this.collector.emit(this.pending.get(msgId),msgId);
	}
}
