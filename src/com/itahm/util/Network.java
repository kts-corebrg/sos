package com.itahm.util;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;

public class Network {

	private int network = 0;
	private int mask = 0;
	private int start; 
	private int remains;
	private String suffix;
	
	public Network(byte [] network, int mask) throws IOException {
		int count = 0;
		
		suffix = Integer.toString(mask);
		
		for (int i=0, _i=network.length; i<_i; i++) {
			this.network = this.network << 8;
			this.network |= 0xff&network[i];
		}
		
		while(count++ < mask) {
			this.mask <<= 1;
			this.mask++;
		}
		
		while(mask++ < 32) {
			this.mask <<= 1;
		}
		
		this.network &= this.mask;
	}
	
	public Network(String network, int mask) throws IOException {
		this(InetAddress.getByName(network).getAddress(), mask);
		
	}
	
	private String toIPString(long ip) {
		return (0xff&(ip >>> 24))+"."+(0xff&(ip >>> 16))+"."+(0xff&(ip >>> 8))+"."+(0xff&ip);
	}
	
	public Iterator<String> iterator() {
		this.start = this.network;
		this.remains = ~this.mask +1;
		
		return new Iterator<String> () {
			
			@Override
			public boolean hasNext() {
				return remains > 0;
			}

			@Override
			public String next() {
				remains--;
				
				return toIPString(start++);
			}
		};
	}

	@Override
	public String toString() {
		return String.format("%s/%s",toIPString(this.network), this.suffix);
	}

}
