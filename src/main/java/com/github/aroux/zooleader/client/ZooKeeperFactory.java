package com.github.aroux.zooleader.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperFactory {

	private final static Pattern SERVER_PATTERN = Pattern.compile("server\\.(\\d+)");

	private final static String CLIENT_PORT_STR = "clientPort";
	private final static String DATA_DIR_STR = "dataDir";
	private final static String MY_ID_FILE = "myid";

	public static ZooKeeper createInstance(String zookeeperPropsFile, int sessionTimeout, Watcher watcher) throws IOException {
		List<ZookeeperNodeAddress> list = extractNodesAddresses(zookeeperPropsFile);
		return new ZooKeeper(toCommaSeparatedString(list), sessionTimeout, watcher);
	}

	private static List<ZookeeperNodeAddress> extractNodesAddresses(String zookeeperPropsFile) throws IOException {
		Properties startupProperties = new Properties();
		startupProperties.load(ClassLoader.getSystemResourceAsStream(zookeeperPropsFile));

		Integer myId = getMyId(startupProperties.getProperty(DATA_DIR_STR) + File.separatorChar + MY_ID_FILE);

		List<ZookeeperNodeAddress> list = new ArrayList<ZookeeperNodeAddress>();
		for (Entry<Object, Object> entry : startupProperties.entrySet()) {
			String key = (String) entry.getKey();
			Matcher matcher = SERVER_PATTERN.matcher(key);
			if (matcher.matches()) {
				int id = Integer.parseInt(matcher.group(1));
				if (id == myId) {
					String address = ((String) entry.getValue()).split(":")[0] + ":" + startupProperties.getProperty(CLIENT_PORT_STR);
					list.add(new ZookeeperNodeAddress(address, id));
				}
			}
		}
		Collections.sort(list);
		return list;
	}

	private static int getMyId(String myIdFile) throws NumberFormatException, IOException {
		BufferedReader reader = new BufferedReader(new FileReader(myIdFile));
		int myId = Integer.parseInt(reader.readLine());
		reader.close();
		return myId;

	}

	private static String toCommaSeparatedString(List<ZookeeperNodeAddress> addresses) {
		StringBuilder hostPortBuff = new StringBuilder();
		boolean first = true;
		for (ZookeeperNodeAddress addr : addresses) {
			if (first) {
				first = false;
			} else {
				hostPortBuff.append(",");
			}
			hostPortBuff.append(addr.getAddress());
		}
		return hostPortBuff.toString();
	}

	private static class ZookeeperNodeAddress implements Comparable<ZookeeperNodeAddress> {

		private final String address;
		private final int id;

		public ZookeeperNodeAddress(String address, int id) {
			super();
			this.address = address;
			this.id = id;
		}

		@Override
		public int compareTo(ZookeeperNodeAddress o) {
			if (this == o) {
				return 0;
			}

			return Integer.valueOf(id).compareTo(Integer.valueOf(o.getId()));
		}

		public String getAddress() {
			return address;
		}

		public int getId() {
			return id;
		}
	}
}
