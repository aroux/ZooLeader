package com.github.aroux.zooleader.server;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

/**
 * 
 * @author Alexandre Roux <alexroux@gmail.com>
 * 
 */
public class ZooLeaderServer {

	private final ExecutorService executorService = Executors.newFixedThreadPool(1);

	private final QuorumPeerMain zooKeeperServer;
	private final QuorumPeerConfig serverConfig;
	private Future<?> serverExecution;

	public ZooLeaderServer(String zookeeperPropsFile) throws IOException {

		Properties startupProperties = new Properties();
		startupProperties.load(ClassLoader.getSystemResourceAsStream(zookeeperPropsFile));

		serverConfig = new QuorumPeerConfig();
		try {
			serverConfig.parseProperties(startupProperties);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		zooKeeperServer = new QuorumPeerMain();
	}

	private void startServer() {
		serverExecution = executorService.submit(new Runnable() {
			@Override
			public void run() {
				try {
					zooKeeperServer.runFromConfig(serverConfig);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		});
	}

	public void start(long timeToWaitForExceptions) throws ExecutionException {
		startServer();
		if (timeToWaitForExceptions > 0) {
			try {
				serverExecution.get(timeToWaitForExceptions, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			} catch (ExecutionException e) {
				throw e;
			} catch (TimeoutException e) {
				// Nothing to do, get returns without exception, all is normal
			}
		}
	}

	public void start() {
		startServer();
	}

	public void stop() throws ExecutionException {
		if (serverExecution != null) {
			try {
				serverExecution.get();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			} catch (ExecutionException e) {
				throw e;
			}
		}
	}
}
