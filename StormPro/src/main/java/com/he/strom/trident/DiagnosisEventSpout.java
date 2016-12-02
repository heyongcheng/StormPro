package com.he.strom.trident;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.tuple.Fields;


public class DiagnosisEventSpout implements ITridentSpout<Long>{

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 2357284982321812396L;
	
	private SpoutOutputCollector collector;
	
	private BatchCoordinator<Long> coordinator = new DefaultCoordinator();
	
	private Emitter<Long> emitter = new DiagnosisEventEmitter();

	public org.apache.storm.trident.spout.ITridentSpout.BatchCoordinator<Long> getCoordinator(String txStateId,
			Map conf, TopologyContext context) {
		return coordinator;
	}

	public org.apache.storm.trident.spout.ITridentSpout.Emitter<Long> getEmitter(String txStateId, Map conf,
			TopologyContext context) {
		return emitter;
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public Fields getOutputFields() {
		return new Fields("event");
	}

}
