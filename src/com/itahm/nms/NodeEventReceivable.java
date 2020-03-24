package com.itahm.nms;

import org.snmp4j.smi.OID;
import org.snmp4j.smi.Variable;

import com.itahm.kts.SeedNode.Protocol;

public interface NodeEventReceivable {
	public void informLimitEvent(int limit);
	public void informPingEvent(long id, long rtt, String protocol);
	public void informResourceEvent(long id, OID oid, OID index, Variable variable);
	public void informSNMPEvent(long id, int code);
	public void informTestEvent(long id, String ip, Protocol protocol, Object result);
}
