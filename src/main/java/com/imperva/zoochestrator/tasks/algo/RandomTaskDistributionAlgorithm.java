package com.imperva.zoochestrator.tasks.algo;

import java.util.Random;

import com.imperva.zoochestrator.tasks.DistributionTask;

/**
 * This implementation randomly assign a task to a worker
 * 
 * @author Doron Lehmann
 *
 */
public class RandomTaskDistributionAlgorithm implements TaskDistributedAlgorithm {

	private Random rand = new Random( System.currentTimeMillis() );

	@Override
	public String getTaskTargetWorker( DistributionTask task ) {
		int index = rand.nextInt( task.workers.size() );
		String designatedWorker = task.workers.get( index ).replaceFirst( task.basePath + "/workers/", "" );
		return designatedWorker;
	}

	@Override
	public void init() {}

}
