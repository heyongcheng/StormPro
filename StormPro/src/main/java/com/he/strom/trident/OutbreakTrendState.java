package com.he.strom.trident;

import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.trident.state.map.NonTransactionalMap;

public class OutbreakTrendState extends NonTransactionalMap<Long> {

	protected OutbreakTrendState(IBackingMap<Long> backing) {
		super(backing);
	}

}
