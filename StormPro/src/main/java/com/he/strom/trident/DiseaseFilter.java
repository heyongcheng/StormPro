package com.he.strom.trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiseaseFilter extends BaseFilter {

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 567689436004038187L;
	private Logger LOG = LoggerFactory.getLogger(DiseaseFilter.class);
	
	public boolean isKeep(TridentTuple tuple) {
		
		Object value = tuple.getValue(0);
		if(value instanceof DiagnosisEvent){
			LOG.info("tuple instance of {}",value.getClass());
			DiagnosisEvent diagnosis = (DiagnosisEvent)value;
			Integer diag = Integer.parseInt(diagnosis.getDiag());
			if(diag <= 322){
				LOG.info("Emitting disease [{}]",diag);
				return true;
			}else{
				LOG.info("Filtering disease [{}]",diag);
				return false;
			}
		}
		LOG.info("[Filtering] tuple instance of {}",value.getClass());
		return false;
	}


}
