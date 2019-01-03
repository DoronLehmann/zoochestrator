package com.imperva.zoochestrator.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.imperva.zoochestrator.TasksSyncManager;
import com.imperva.zoochestrator.tasks.Task;
import com.imperva.zoochestrator.tasks.TaskAssignmentCallback;
import com.imperva.zoochestrator.tasks.algo.RoundRobinDistributionAlgorithm;

/**
 * 
 * @author Doron Lehmann
 *
 */
public class ZooChestratorExample {

	private static final Logger logger = LoggerFactory.getLogger( ZooChestratorExample.class );

	public static void main( String[] args ) throws Exception {

		final String id = "my.server.1";
		final String scope = "/cats";
		List<String> zkServersIps = new ArrayList<>();
		zkServersIps.add( "192.168.0.1" );

		// Create and initialize the tasksSyncManager
		final TasksSyncManager tasksSyncManager = new TasksSyncManager();
		tasksSyncManager.init( zkServersIps, id, "Doron", "testUser", "testPassword", false );

		// define the callback logic
		TaskAssignmentCallback callback = new TaskAssignmentCallback() {
			@Override
			public void execute( Task task ) {
				System.out.println( "I got a new task - " + task.event.getData().getPath() );
				try {
					// create a new task based on the received task information
					Task newTask = new Task( task.data.substring( task.data.lastIndexOf( "-" ) + 1 ) + "-" + System.currentTimeMillis(), null );
					// get the account ID from the task metadata
					String metadataItemFromTask = Task.getMetadataItemFromTask( task.event.getData().getPath(), "accountId" );
					Long accountIdFromTask = 0L;
					if ( metadataItemFromTask != null ) {
						accountIdFromTask = Long.valueOf( metadataItemFromTask );
					}
					Map<String, String> metaMap = new HashMap<>();
					metaMap.put( "accountId", String.valueOf( accountIdFromTask + 1 ) );
					// add metadata to the new task
					newTask.setMetadata( metaMap );
					// delete the current task and create a new task
					tasksSyncManager.deleteAndCreateFollowingTask( scope, task, newTask );
				}
				catch ( Exception e ) {
					logger.error( e.getMessage() );
				}
			}
		};

		// register to the task sync manager
		tasksSyncManager.register( scope, true, new RoundRobinDistributionAlgorithm(), callback );

		// create the first task for the scope
		Task firstTask = new Task( id, null );
		Map<String, String> metaMap = new HashMap<>();
		metaMap.put( "accountId", String.valueOf( 1 ) );
		firstTask.setMetadata( metaMap );
		tasksSyncManager.createTask( scope, firstTask );
		System.out.println( "Taking a 10 second sleep...Zzz..." );
		Thread.sleep( 10000 );
		System.out.println( "I'm up!" );
		// close the task sync manager
		tasksSyncManager.close();
	}

}
