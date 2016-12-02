package com.he.strom.trident;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CityAssignment implements Function{

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 7864450004597720119L;
	private Logger LOG = LoggerFactory.getLogger(CityAssignment.class);
	
	private static Map<String,double[]> CITIES = new HashMap<String,double[]>();
	static{
		double[] phl = {39.875365,-75.249524};
		CITIES.put("PHL", phl);
		double[] nyc = {40.71448,-74.00598};
		CITIES.put("NYC", nyc);
		double[] sf = {-31.4250142,-62.0841809};
		CITIES.put("SF", sf);
		double[] la = {-34.05374,-118.24307};
		CITIES.put("LA", la);
	}
	
	public void prepare(Map conf, TridentOperationContext context) {
		
	}

	public void cleanup() {
		
	}

	public void execute(TridentTuple tuple, TridentCollector collector) {
		DiagnosisEvent diagnosis = (DiagnosisEvent)tuple.getValue(0);
		double leastDistance = Double.MAX_VALUE;
		String closestCity = "NONE";
		Set<Entry<String, double[]>> entrySet = CITIES.entrySet();
		for(Entry<String,double[]> city : entrySet){
			double R = 6371;
			double x = (city.getValue()[0] - diagnosis.getLng()) *
					Math.cos((city.getValue()[0] + diagnosis.getLng()) / 2);
			double y = (city.getValue()[1] - diagnosis.getLat());
			double d = Math.sqrt(x * x + y * y) * R;
			if(d < leastDistance){
				leastDistance = d;
				closestCity = city.getKey();
			}
		}
		LOG.info("Closest city to lag=[{}] , lng=[{}] == [{}] , d=[{}]",
				diagnosis.getLat(),diagnosis.getLng(),closestCity,leastDistance);
		List<Object> values = new ArrayList<Object>();
		values.add(closestCity);
		collector.emit(values);
	}

}
