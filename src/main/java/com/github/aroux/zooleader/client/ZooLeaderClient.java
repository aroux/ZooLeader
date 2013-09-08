package com.github.aroux.zooleader.client;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.github.aroux.zooleader.algorithm.LeaderNotifier;
import com.github.aroux.zooleader.algorithm.LeaderService;

/**
 * 
 * @author Alexandre Roux <alexroux@gmail.com>
 * 
 */
public class ZooLeaderClient implements Watcher {

	private LeaderService leaderService;

	private final LeaderNotifier leaderNotifier;

	public ZooLeaderClient(LeaderNotifier leaderNotifier) throws IOException {
		this.leaderNotifier = leaderNotifier;
	}

	public void setLeaderService(LeaderService leaderService) {
		this.leaderService = leaderService;
	}

	public void start() throws InterruptedException {
		if (leaderService == null) {
			throw new IllegalStateException("LeaderService has not been set.");
		}
		leaderService.start(leaderNotifier);
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() != KeeperState.SyncConnected) {
			if (leaderService != null) {
				leaderService.notifySelfFailure();
			}
		}
	}

}
