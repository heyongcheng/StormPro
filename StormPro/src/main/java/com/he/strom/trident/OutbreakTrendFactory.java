package com.he.strom.trident;

import java.util.Map;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutbreakTrendFactory implements StateFactory {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 6193627109553887530L;
	
	private Logger LOG = LoggerFactory.getLogger(OutbreakTrendFactory.class);
	
	public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
		return new OutbreakTrendState(new OutbreakTrendBackingMap());
	}

}
