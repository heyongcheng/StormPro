package com.he.strom.trident;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutbreakDetector implements Function {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1905981091495610176L;
	
	private static final int THRESHOLD = 10000;
	
	private Logger LOG = LoggerFactory.getLogger(OutbreakDetector.class);
	
	public void prepare(Map conf, TridentOperationContext context) {
		
	}

	public void cleanup() {
		
	}

	public void execute(TridentTuple tuple, TridentCollector collector) {
		String key = (String)tuple.getValue(0);
		Long count = (Long)tuple.getValue(1);
		if(count > THRESHOLD){
			List<Object> values = new ArrayList<Object>();
			values.add("Outbreak detected for[" + key + "]");
			collector.emit(values);
		}
	}

}
