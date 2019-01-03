package com.imperva.zoochestrator.tasks.algo;

import com.imperva.zoochestrator.tasks.DistributionTask;

/**
 * An interface for implementing task distribution algorithms
 * 
 * @author Doron Lehmann
 *
 */
public interface TaskDistributedAlgorithm {

	/**
	 * Gets a task target worker name according to the implemented algorithm 
	 * @param task a DistributionTask object
	 * @return the destination worker name
	 */
	public String getTaskTargetWorker( DistributionTask task ) throws Exception;
	
	
	/**
	 * Initialize the algorithm instance
	 */
	public void init();

}
