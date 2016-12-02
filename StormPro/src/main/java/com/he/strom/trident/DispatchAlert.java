package com.he.strom.trident;

import java.util.Map;

import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatchAlert implements Function {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = -5771729450774593526L;
	
	private Logger LOG = LoggerFactory.getLogger(DispatchAlert.class);
	
	public void prepare(Map conf, TridentOperationContext context) {
		
	}

	public void cleanup() {
		
	}

	public void execute(TridentTuple tuple, TridentCollector collector) {
		String alert = (String)tuple.getValue(0);
		LOG.error("ALERT RECEIVED [{}]",alert);
		LOG.error("Dispatch the nation guard!");
		System.exit(0);
		
	}

}
