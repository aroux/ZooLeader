package com.github.aroux.zooleader.algorithm;

import java.util.SortedSet;

/**
 * 
 * @author Alexandre Roux <alexroux@gmail.com>
 * 
 */
public interface DistributedPrimitivesService {

	void init() throws InterruptedException;

	int proposeLeadership() throws InterruptedException;

	SortedSet<Integer> getParticipantIds() throws InterruptedException;

	boolean registerTo(int id, ParticipantFailureListener listener) throws InterruptedException;

}
