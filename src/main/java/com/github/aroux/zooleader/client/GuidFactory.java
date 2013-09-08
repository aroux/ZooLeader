package com.github.aroux.zooleader.client;

import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.UnknownHostException;

public class GuidFactory {

	public static long createInstance() throws NumberFormatException, UnknownHostException {
		int pid = Integer.parseInt(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
		int ip = Integer.parseInt(Inet4Address.getLocalHost().getHostAddress().replace(".", ""));
		return Long.parseLong(String.valueOf(ip) + String.valueOf(pid));
	}
}
