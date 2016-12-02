package com.he.strom.trident;

import java.io.Serializable;

import org.apache.storm.trident.spout.ITridentSpout.BatchCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultCoordinator implements BatchCoordinator<Long>,Serializable {

	Logger LOG = LoggerFactory.getLogger(DefaultCoordinator.class);
	
	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = -4504349697280760189L;

	public Long initializeTransaction(long txid, Long prevMetadata, Long currMetadata) {
		if(LOG.isInfoEnabled()){
			LOG.info("initializeTransaction [{}]",txid);
		}
		return null;
	}

	public void success(long txid) {
		if(LOG.isInfoEnabled()){
			LOG.info("successTransaction [{}]",txid);
		}
	}

	public boolean isReady(long txid) {
		return true;
	}

	public void close() {

	}

}
