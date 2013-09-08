package com.github.aroux.zooleader.algorithm.impl;

import java.util.SortedSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.github.aroux.zooleader.algorithm.DistributedPrimitivesService;
import com.github.aroux.zooleader.algorithm.LeaderNotifier;
import com.github.aroux.zooleader.algorithm.LeaderService;
import com.github.aroux.zooleader.algorithm.ParticipantFailureListener;

/**
 * Implement algorithm from zookeeper recipe :
 * http://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection
 * 
 * @author Alexandre Roux <alexroux@gmail.com>
 * 
 */
public class LeaderServiceImpl implements LeaderService, ParticipantFailureListener {

	private final static Logger logger = Logger.getLogger(LeaderServiceImpl.class);

	private final DistributedPrimitivesService distributedPrimitivesService;

	private final ExecutorService executorService;

	private LeaderNotifier notifier;

	int currentId = Integer.MAX_VALUE;

	public LeaderServiceImpl(DistributedPrimitivesService distributedPrimitivesService, ThreadFactory threadFactory) {
		this.distributedPrimitivesService = distributedPrimitivesService;
		this.executorService = Executors.newSingleThreadExecutor(threadFactory);
	}

	@Override
	public void start(LeaderNotifier notifier) throws InterruptedException {
		distributedPrimitivesService.init();
		this.notifier = notifier;
		bootstrap();
	}

	synchronized private void bootstrap() throws InterruptedException {
		logger.debug("Bootstrap");
		propose();
		SortedSet<Integer> ids = collect();
		decide(ids);
	}

	private void propose() throws InterruptedException {
		currentId = distributedPrimitivesService.proposeLeadership();
		logger.debug("Proposing with id " + currentId);
	}

	private SortedSet<Integer> collect() throws InterruptedException {
		SortedSet<Integer> participantIds = distributedPrimitivesService.getParticipantIds();
		logger.debug("Collect participants ids : " + participantIds);
		return participantIds;
	}

	private void decide(SortedSet<Integer> ids) throws InterruptedException {
		SortedSet<Integer> lowerIds = ids.headSet(currentId);
		if (lowerIds.size() == 0) {
			logger.debug("No lower id, become leader.");
			// Current participant is leader
			executorService.submit(new NotifierTask(true));
		} else {
			// Current participant is not leader, watch for participant with
			// consecutive lower id
			int id = lowerIds.last();
			logger.debug("Lower id found, watch participant with id : " + id);
			distributedPrimitivesService.registerTo(id, this);
		}
	}

	@Override
	synchronized public void onFailure(int id) {
		try {
			if (id != currentId) {
				logger.debug("Participant with id " + id + " has failed. Try to find a new leader.");
				SortedSet<Integer> ids = collect();
				decide(ids);
			} else {
				// TODO should never happen
				throw new NotImplementedException();
			}
		} catch (InterruptedException e) {
			// TODO : handle this
		}
	}

	private class NotifierTask implements Runnable {

		private final boolean leader;

		public NotifierTask(boolean leader) {
			this.leader = leader;
		}

		@Override
		public void run() {
			notifier.notifyLeadership(leader);
		}

	}

	@Override
	public void notifySelfFailure() {
		logger.debug("Self failure.");
		// Self failure, stop to be leader
		executorService.submit(new NotifierTask(false));

		logger.debug("Try to bootsrap again");
		// TODO : wait sometime before bootstrapping again
		try {
			bootstrap();
		} catch (InterruptedException e) {
			// TODO try again after sometime
		}
	}

	@Override
	public void stop() {
		executorService.shutdown();
	}

}
