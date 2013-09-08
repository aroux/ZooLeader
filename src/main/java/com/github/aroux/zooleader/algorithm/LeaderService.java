package com.github.aroux.zooleader.algorithm;

/**
 * 
 * @author Alexandre Roux <alexroux@gmail.com>
 * 
 */
public interface LeaderService {

	/**
	 * Starts the leader service, returning only when proposal phase of leader
	 * election algorithm has been decided. The notifier given as argument is
	 * called each time a change in leadership occurs which impacts current
	 * participant.
	 * 
	 * @param notifier
	 *            the notifier to call
	 * @throws InterruptedExecption
	 *             throws when the process has been interrupted
	 */
	void start(LeaderNotifier notifier) throws InterruptedException;

	void notifySelfFailure();

	void stop();

}
