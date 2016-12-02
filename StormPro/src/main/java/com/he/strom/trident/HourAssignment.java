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

public class HourAssignment implements Function {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 5835936342805135799L;
	
	private Logger LOG = LoggerFactory.getLogger(HourAssignment.class);
	
	public void prepare(Map conf, TridentOperationContext context) {
		
	}

	public void cleanup() {

	}

	public void execute(TridentTuple tuple, TridentCollector collector) {
		DiagnosisEvent diagnosis = (DiagnosisEvent)tuple.getValue(0);
		
		String city = (String)tuple.getValue(1);
		Long time = diagnosis.getTime();
		Long hourSinceEpoch = time / 1000 / 60 / 60;
		String key = city + ":" + diagnosis.getDiag() + ":" + hourSinceEpoch;
		
		List<Object> values = new ArrayList<Object>();
		values.add(hourSinceEpoch);
		values.add(key);
		
		collector.emit(values);
	}

}
