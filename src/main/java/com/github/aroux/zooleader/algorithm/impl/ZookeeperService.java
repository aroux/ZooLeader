package com.github.aroux.zooleader.algorithm.impl;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.github.aroux.zooleader.algorithm.DistributedPrimitivesService;
import com.github.aroux.zooleader.algorithm.ParticipantFailureListener;

/**
 * 
 * @author Alexandre Roux <alexroux@gmail.com>
 * 
 */
public class ZookeeperService implements DistributedPrimitivesService {

	private final static String DEFAULT_BASE_PATH = "/ELECTION";

	private final String basePath;

	private final String currentParticipantPath;

	private final long guid;

	private final ZooKeeper zooKeeper;

	@Override
	public void init() throws InterruptedException {
		try {
			if (zooKeeper.exists(basePath, null) == null) {
				byte data[] = new byte[0];
				zooKeeper.create(basePath, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		}
	}

	public ZookeeperService(ZooKeeper zooKeeper, String basePathSuffix, long guid) {
		this.guid = guid;
		this.zooKeeper = zooKeeper;
		basePath = DEFAULT_BASE_PATH + "_" + basePathSuffix;
		currentParticipantPath = basePath + "/" + guid + "_";
	}

	@Override
	public int proposeLeadership() throws InterruptedException {
		byte data[] = new byte[0];
		try {
			String path = zooKeeper.create(currentParticipantPath, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			return extractIdFromPath(path);
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public SortedSet<Integer> getParticipantIds() throws InterruptedException {
		try {
			List<String> children = zooKeeper.getChildren(basePath, null);
			SortedSet<Integer> participantIds = new TreeSet<Integer>();
			for (String path : children) {
				participantIds.add(extractIdFromPath(path));
			}
			return participantIds;
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean registerTo(int id, ParticipantFailureListener listener) throws InterruptedException {
		try {
			List<String> children = zooKeeper.getChildren(basePath, null);
			for (String path : children) {
				int participantId = extractIdFromPath(path);
				if (id == participantId) {
					// Participant asked to be watched
					try {
						zooKeeper.getChildren(basePath + "/" + path, new WatcherAdapter(id, listener));
						return true;
					} catch (KeeperException e) {
						if (e.code() == Code.NONODE) {
							// Node doesn't exist anymore
							break;
						}
					}
				}
			}
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		}

		return false;
	}

	private int extractIdFromPath(String path) {
		String split[] = path.split("_");
		return Integer.parseInt(split[split.length - 1]);
	}

	private class WatcherAdapter implements Watcher {

		private final ParticipantFailureListener listener;

		private final int id;

		public WatcherAdapter(int id, ParticipantFailureListener listener) {
			this.listener = listener;
			this.id = id;
		}

		@Override
		public void process(WatchedEvent event) {
			if (event.getType() == EventType.NodeDeleted) {
				listener.onFailure(id);
			}
		}
	}
}
