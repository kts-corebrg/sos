package com.itahm.kts;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import com.itahm.http.Request;
import com.itahm.http.Response;
import com.itahm.http.Response.Status;
import com.itahm.json.JSONException;
import com.itahm.json.JSONObject;
import com.itahm.service.Serviceable;

public class KeepAlive implements Serviceable {

	private Boolean isClosed = true;
	public static final Map<String, Data> map = new HashMap<>();
	
	public static class Data {
		public long lastResponse = System.currentTimeMillis();
		public String address = null;
	}
	
	public KeepAlive(Path path) {
		
	}
	
	@Override
	public void start() {
		synchronized(this.isClosed) {
			if (!this.isClosed) {
				return;
			}
			
			this.isClosed = false;
		}
	}

	@Override
	public void stop() {
		synchronized(this.isClosed) {
			if (this.isClosed) {
				return;
			}
			
			this.isClosed = true;
		}
	}

	@Override
	public boolean isRunning() {
		synchronized(this.isClosed) {
			return !this.isClosed;
		}
	}

	@Override
	public boolean service(Request request, Response response, JSONObject data) {
		synchronized(this.isClosed) {
			if (this.isClosed) {
				return false;
			}
		}
		
		switch (data.getString("command").toUpperCase()) {
		case "SOS":
			String key = data.getString("key");
			
			if (key.length() == 0) {
				throw new JSONException("Invalid key string.");
			}
			
			if (map.containsKey(key)) {
				Data ka = map.get(key);
				
				if (ka == null) {
					response.setStatus(Status.SERVERERROR);
					
					map.remove(key);
				} else {
					ka.lastResponse = System.currentTimeMillis();
					ka.address = request.getRemoteAddr();
				}
				
			} else {
				throw new JSONException("Not registered key.");
			}
			
			return true;
		}
		
		return false;
	}

}
