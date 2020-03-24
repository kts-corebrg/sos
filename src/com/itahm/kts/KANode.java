package com.itahm.kts;

import java.io.IOException;

import com.itahm.nms.node.Node;

public class KANode extends Node {
	private final static long DELAY = 10000L;
	
	public final String key;
	
	public KANode(long id, String key) {
		super(id, String.format("KANode %s", key));
		
		this.key = key;
	}

	@Override
	public boolean isReachable() throws IOException {
		KeepAlive.Data ka = KeepAlive.map.get(this.key);
		
		if (ka != null && System.currentTimeMillis() - ka.lastResponse < DELAY) {
			return true;
		}
		
		try {
			Thread.sleep(super.timeout);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		
		return false;
	}

}
