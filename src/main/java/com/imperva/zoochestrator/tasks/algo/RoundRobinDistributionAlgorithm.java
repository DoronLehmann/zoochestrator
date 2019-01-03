package com.imperva.zoochestrator.tasks.algo;

import java.util.Random;

import com.imperva.zoochestrator.tasks.DistributionTask;
import com.imperva.zoochestrator.tasks.Task;
import com.imperva.zoochestrator.utils.ZookeeperUtils;

/**
 * This implementation assigns a task to a worker using a simple round-robin algorithm, keeping the latest worker index ,in the workers collection, on the distribution task
 *
 * @author Moran Bar
 *
 */
public class RoundRobinDistributionAlgorithm implements TaskDistributedAlgorithm {

	private Random rand = new Random( System.currentTimeMillis() );
	
	@Override
	public String getTaskTargetWorker( DistributionTask task ) {
		String previousWorker = extractPreviousWorker(task.taskPath);
		int index = extractWorkerIndex(previousWorker, task);
		if (index > -1) {
			//Previous worker is in the list
			index = (index + 1) % task.workers.size(); 
		} 
		else {
			//Task missing the previous worker's information( potentially the task was only created ) or worker is not part of the distribution task's workers list
			index = chooseInitialWorker(task);
		}
		String designatedWorker = extractWorkerNameFromPath(task.workers.get( index ), task.basePath);
		return designatedWorker;
	}

	@Override
	public void init() {}
	
	private String extractPreviousWorker(String taskPath){
		return Task.getMetadataItemFromTask( taskPath, "workerId" );
	}
	
	private int extractWorkerIndex(String worker, DistributionTask task){
		int index = -1;
		if (ZookeeperUtils.isNotEmpty(worker)) {
			for (int i = 0; i < task.workers.size(); i++) {
				//ZK task metadata values currently doesn't support dot-delimited strings. 
				//Assumptions: 
				//		1.server( which serve as workers in the ZK namespaces ) are built as dot-delimited strings
				//		2.The dot delimiters were dropped prior to inserting the server name as the workerId metadata.
				String taskWorkerName = extractWorkerNameFromPath(task.workers.get(i), task.basePath).replace(".", "");
				taskWorkerName = taskWorkerName.substring(taskWorkerName.lastIndexOf("worker-") + 7);//Trim the worker prefix the curator adds to the worker ids.
				if (taskWorkerName.equals(worker)) {
					return i;
				}
			}
		} 
		return index;
	}
	
	private int chooseInitialWorker(DistributionTask task ){
		return rand.nextInt( task.workers.size() );
	}
	
	private String extractWorkerNameFromPath(String workerPath, String basePath){
		return workerPath.replaceFirst( basePath + "/workers/", "" );
	}

}
