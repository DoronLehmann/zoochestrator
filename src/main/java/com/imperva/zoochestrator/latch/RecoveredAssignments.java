package com.imperva.zoochestrator.latch;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a task to recover assignments after <br>
 * a primary master crash. The main idea is to determine the <br> 
 * tasks that have already been assigned and assign the ones that haven't 
 * 
 * @author Doron Lehmann
 *
 */
public class RecoveredAssignments {

	private static final Logger logger = LoggerFactory.getLogger( RecoveredAssignments.class );
	private RecoveryCallback cb;
	private CuratorFramework client;
	private String basePath;

	/**
	 * Callback interface. Called once 
	 * recovery completes or fails.
	 *
	 */
	public interface RecoveryCallback {
		final static int OK = 0;
		final static int FAILED = -1;

		public void recoveryComplete( int rc, List<String> tasks );
	}

	/**
	 * Recover unassigned tasks.
	 * @param client
	 * @param path
	 */
	public RecoveredAssignments( CuratorFramework client, String path ) {
		this.client = client;
		this.basePath = path;
	}

	/**
	 * Starts recovery.
	 * 
	 * @param recoveryCallback
	 * @throws Exception 
	 */
	public void recover( RecoveryCallback recoveryCallback ) throws Exception {
		logger.info( "Starting to recover tasks..." );
		cb = recoveryCallback;
		// Current tasks
		List<String> tasks = client.getChildren().forPath( basePath + "/tasks" );
		// current assigned workers
		List<String> assignedWorkers = client.getChildren().forPath( basePath + "/assign" );
		// current live workers
		List<String> workers = client.getChildren().forPath( basePath + "/workers" );
		// if there are no workers - really an edge case - should not happen
		if ( workers.size() == 0 ) {
			logger.warn( "Empty list of workers, possibly just starting" );
			cb.recoveryComplete( RecoveryCallback.OK, new ArrayList<String>() );
			return;
		}
		else {
			// check all the workers under the /assign znode
			for ( String assignedWorker : assignedWorkers ) {
				// if the tasks belongs to a worker that no linger exists - it should be re-assigned
				if ( !workers.contains( assignedWorker ) ) {
					recoverAbsentWorkerTasks( client, basePath, assignedWorker, tasks, true );
				}
			}
			logger.info( "finished the recover tasks call - total tasks to recover is {}", tasks.size() );
			cb.recoveryComplete( RecoveryCallback.OK, tasks );
		}
	}
	
	/**
	 * Recovers an absent worker tasks
	 * @param client
	 * @param basePath
	 * @param assignedWorkerName
	 */
	public static void recoverAbsentWorkerTasks( CuratorFramework client, String basePath, String assignedWorkerName ) {
		recoverAbsentWorkerTasks( client, basePath, assignedWorkerName, null, false );
	}
	
	/**
	 * Recovers an absent worker tasks
	 * @param client
	 * @param basePath
	 * @param assignedWorkerName
	 * @param tasksList
	 * @param updateTasksList
	 */
	private static void recoverAbsentWorkerTasks( CuratorFramework client, String basePath, String assignedWorkerName, List<String> tasksList, boolean updateTasksList ) {
		try {
			// get each assignment znode tasks
			List<String> assigendTasks = client.getChildren().forPath( basePath + "/assign/" + assignedWorkerName );
			logger.info( "Recovering {} tasks from assigned worker {}", assigendTasks.size(), assignedWorkerName );
			for ( String task : assigendTasks ) {
				handleMissingWorkerTask( client, basePath, task, assignedWorkerName, tasksList, updateTasksList );
			}
			// once all the tasks has been reassigned - deleted the assignment znode
			logger.info( "Deleting node /assign/{}", assigendTasks );
			client.delete().guaranteed().forPath( basePath + "/assign/" + assignedWorkerName );
			logger.info( "Deleted node /assign/{}", assigendTasks );
		}
		catch ( Exception e ) {
			logger.error( "Failed to recover tasks from assigned worker {}", assignedWorkerName, e );
		}
	}
	
	private static void handleMissingWorkerTask( CuratorFramework client, String basePath, String task, String assignedWorkerName, List<String> tasksList, boolean updateTasksList ) {
		try {
			// recreate each task under the /tasks znode and delete if from the assignment znode
			client.inTransaction().
				create().withMode( CreateMode.PERSISTENT ).
				forPath( basePath + "/tasks/" + task, client.getData().forPath( basePath + "/assign/" + assignedWorkerName + "/" + task ) ).
				and().
				delete().
				forPath( basePath + "/assign/" + assignedWorkerName + "/" + task ).
				and().
				commit();
			logger.info( "Moved task {} from /assign/{} to tasks/{}", task, assignedWorkerName, task );
			if ( updateTasksList ) {
				// add the tasks to the tasks list
				tasksList.add( task );
			}
		}
		catch ( Exception e ) {
			logger.error( "Failed to complete transaction for assigning task {} to the tasks node and the delete it from worker {}", task, assignedWorkerName, e );
		}
	}
}
