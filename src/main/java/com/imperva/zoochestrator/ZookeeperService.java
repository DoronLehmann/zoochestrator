package com.imperva.zoochestrator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.imperva.zoochestrator.callbacks.LeadershipStatusChangeCallback;
import com.imperva.zoochestrator.callbacks.ZooKeeperChildrenEventCallback;
import com.imperva.zoochestrator.latch.CuratorLeadershipLatch;
import com.imperva.zoochestrator.latch.CuratorMasterLatch;
import com.imperva.zoochestrator.tasks.algo.TaskDistributedAlgorithm;

/**
 * A general service for getting a ZooKeeper CuratorFramework client.
 * 
 * @author Doron Lehmann
 *
 */
public class ZookeeperService {

	private static final Logger logger = LoggerFactory.getLogger( ZookeeperService.class );

	/**
	 *  Create and starts a new CuratorFramework client
	 * @param zkServersIps a list of ZooKeeper servers IPs
	 * @param namespace used to set a prefix
	 * @param username to connect to zookeeper
	 * @param password to connect to zookeeper
	 * @return a new CuratorFramework client. On error returns null.
	 */
	public static CuratorFramework createAndStartClient( List<String> zkServersIps, String namespace, String username, String password ) {
		return createAndStartClient( zkServersIps, namespace, username, password, false );
	}
	

	/**
	 * Create and starts a new CuratorFramework client
	 * @param zkServersIps a list of ZooKeeper servers IPs
	 * @param namespace namespace used to set a prefix
	 * @param username to connect to zookeeper
	 * @param password to connect to zookeeper
	 * @param useLocalServers determines if to ignore the local config and connect to what is defined in "zookeeper.hosts.list"
	 * @return a new CuratorFramework client. On error returns null.
	 */
	public static CuratorFramework createAndStartClient( List<String> zkServersIps, String namespace, String username, String password, boolean useLocalServers ) {
		logger.info( "Createing CuratorFramework client" );
		if ( namespace != null && !namespace.isEmpty() && namespace.equalsIgnoreCase( "zookeeper" ) ) {
			throw new IllegalArgumentException( "Namespace can't be 'zookeeper' " );
		}
		// create a new builder
		Builder curatorFrameworkBuilder = CuratorFrameworkFactory.builder();
		curatorFrameworkBuilder.connectString( getZKServersConnectionString( zkServersIps, useLocalServers ) );
		// set the retry policy
		curatorFrameworkBuilder.retryPolicy( new ExponentialBackoffRetry( 1000, 5 ) );
		// set a namespace
		if ( namespace != null && !namespace.isEmpty() ) {
			curatorFrameworkBuilder.namespace( namespace );
		}
		// build the client and start it
		CuratorFramework client = curatorFrameworkBuilder.build();
		client.getUnhandledErrorListenable().addListener( errorsListener );
		logger.info( "Starting the CuratorFramework client" );
		int maxBlockTime = Integer.parseInt( System.getProperty( "zookeeper.curator.block.until.connected.max.time.in.seconds", "5" ) );
		client.start();
		boolean connected = false;
		try {
			logger.info( "Checking for connection to zookeeper..." );
			connected = client.blockUntilConnected( maxBlockTime, TimeUnit.SECONDS );
		}
		catch ( InterruptedException e ) {
			logger.error( "CuratorFramework client faild to connect to zookeeper - Max time for trying is {} seconds", maxBlockTime, e );
		}
		if ( !connected ) {
			logger.error( "Failed to connect to zookeeper - Max time for trying is {} seconds", maxBlockTime );
			closeClient( client );
			return null;
		}
		logger.info( "Successfully connected to zookeeper!!!" );
		logger.info( "CuratorFramework started" );
		return client;
	}
	

	/**
	 * Closes a specific CuratorFramework client
	 * @param client
	 */
	public static void closeClient( CuratorFramework client ) {
		logger.info( "Closing the CuratorFramework client" );
		if ( client != null ) {
			client.close();
			try {
				logger.info( "Taking a sleep in order to make sure all connections are close" );
				Thread.sleep( Integer.parseInt( System.getProperty( "zookeeper.close.client.sleep.timeinmillis", "10000" ) ) );
				logger.info( "Closed the CuratorFramework client" );
			}
			catch ( InterruptedException e ) {
				logger.error( "Failed to take a 5 seconds sleep in order to make sure all connections are close", e );
			}
		}
		else {
			logger.error( "Failed to close zookeeper client - passed client is null" );
		}
	}

	/**
	 * Creates a CuratorMasterLatch which runs for master and manages workers tasks when he is the leader.
	 * When the work with the latch s done, its close() method should be called.
	 * @param client the CuratorFramework
	 * @param id the id of the registered application
	 * @param scope the scope of the tasks that should be managed - must start with a backslash "/"
	 * @return CuratorMasterLatch. in order to wait for mastership you should call awaitLeadership
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	public static CuratorMasterLatch createAndStartMasterLatch( CuratorFramework client, String id, String scope, TaskDistributedAlgorithm taskDistributionAlgorithm, boolean runForMaster ) throws IllegalArgumentException, Exception {
		return createAndStartMasterLatch( client, id, scope, taskDistributionAlgorithm, runForMaster, new HashMap<String, Object>() );
	}

	/**
	 * Creates a CuratorMasterLatch which runs for master and manages workers tasks when he is the leader.
	 * When the work with the latch s done, its close() method should be called.
	 * @param client the CuratorFramework
	 * @param id the id of the registered application
	 * @param scope the scope of the tasks that should be managed - must start with a backslash "/"
	 * @param taskDistributionAlgorithm the algo. for distributing the tasks
	 * @param runForMaster true if the manager should run for master role, <br> false for workers scenario which do not want to acquire leadership
	 * @param workerNodeExtraData to be set on the registered system worker node
	 * @return CuratorMasterLatch. in order to wait for mastership you should call awaitLeadership
	 * @throws IllegalArgumentException
	 * @throws Exception
	 */
	public static CuratorMasterLatch createAndStartMasterLatch( CuratorFramework client, String id, String scope, TaskDistributedAlgorithm taskDistributionAlgorithm, boolean runForMaster, Map<String, Object> workerNodeExtraData ) throws IllegalArgumentException, Exception {
		logger.info( "Creating CuratorMasterLatch for {} ...", scope );
		CuratorMasterLatch curatorMasterLatch = new CuratorMasterLatch( client, id, scope, taskDistributionAlgorithm, runForMaster, workerNodeExtraData );
		logger.info( "Created the CuratorMasterLatch for {}", scope );
		return curatorMasterLatch;
	}
	
	/**
	 * Creates a CuratorLeadershipLatch which runs for leadership.
	 * When the work with the latch s done, its close() method should be called.
	 * @param client the CuratorFramework
	 * @param id the id of the registered application
	 * @param scope the scope of the tasks that should be managed - must start with a backslash "/"
	 * @return CuratorLeadershipLatch. in order to wait for mastership you should call awaitLeadership
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	public static CuratorLeadershipLatch createAndStartLeadershipLatch( CuratorFramework client, String id, String scope, LeadershipStatusChangeCallback callback ) throws IllegalArgumentException, Exception {
		logger.info( "Creating CuratorLeadershipLatch for {} ...", scope );
		CuratorLeadershipLatch curatorLeadershipLatch = new CuratorLeadershipLatch( client, id, scope, callback );
		logger.info( "Created the CuratorLeadershipLatch for {}", scope );
		return curatorLeadershipLatch;
	}

	/**
	 * Creates a watcher and listener for the given scope tasks.
	 * When a new task is assigned, the provided callback is called.
	 * You need to note that once the callback has been executed, the callback is in charge for deleting the task once it has been completed.
	 * @param client the CuratorFramework
	 * @param id
	 * @param scope
	 * @param callback
	 * @throws Exception
	 */
	public static void createAndAddTasksAssignmentListener( CuratorFramework client, final String id, final String scope, final ZooKeeperChildrenEventCallback callback ) throws Exception {
		logger.info( "Createing Tasks Assignment Listener for {}", scope );
		
		/* This method is a bit complex.
		 * It first create a listener on the assign node (for workers).
		 * When a worker with the right id is created, a listener on that worker's tasks is created.
		 * 
		 * This complexity is necessary, since the worker node (in the assign node) can be deleted by the master when disconnected from ZK.
		 * In that scenario the tasks' listener must be updated
		 */
		
		PathChildrenCache assignmentWorkers = createTasksAssignmentWorkersWatcher( client, scope );
		PathChildrenCacheListener listener = new PathChildrenCacheListener() {
			private PathChildrenCache tasksCache = null;
			
			@Override
			public void childEvent( CuratorFramework client, PathChildrenCacheEvent event ) throws Exception {
				// The worker node itself has changed
				switch ( event.getType() ) {
					case CHILD_ADDED: {
						if ( event.getData().getPath().endsWith( CuratorMasterLatch.generateWorkerId( id ) ) ) {
							if ( tasksCache != null ) {
								logger.info( "Assingments worker node has changed" );
								tasksCache.close();
							}
							tasksCache = createTasksAssignmentWatcher( client, id, scope );
							PathChildrenCacheListener listener = new PathChildrenCacheListener() {
								
								@Override
								public void childEvent( CuratorFramework client, PathChildrenCacheEvent event ) throws Exception {
									// a new task
									switch ( event.getType() ) {
										case CHILD_ADDED: {
											callback.execute( event );
											break;
										}
										default:
											break;
									}
								}
							};
							tasksCache.getListenable().addListener( listener );
						}

						break;
					}
					default:
						break;
				}
			}
		};
		assignmentWorkers.getListenable().addListener( listener );
	}
	
	private static UnhandledErrorListener errorsListener = new UnhandledErrorListener() {
		public void unhandledError( String message, Throwable e ) {
			logger.error( "Curator Client Error: {}", message, e );
		}
	};

	private static PathChildrenCache createTasksAssignmentWatcher( CuratorFramework client, String id, String scope ) throws Exception {
		String path = scope + "/assign/" + CuratorMasterLatch.generateWorkerId(id);
		PathChildrenCache childrenCache = new PathChildrenCache( client, path, false );
		childrenCache.start();
		return childrenCache;
	}
	
	private static PathChildrenCache createTasksAssignmentWorkersWatcher( CuratorFramework client, String scope ) throws Exception {
		String path = scope + "/assign";
		PathChildrenCache childrenCache = new PathChildrenCache( client, path, false );
		childrenCache.start();
		return childrenCache;
	}
	
	private static String getZKServersConnectionString( List<String> zkServersIps, boolean useLocalServers ) {
		List<String> hostnames = new LinkedList<String>();
		List<String> defaultServers = new ArrayList<>();
		String defaultServersList = System.getProperty( "zookeeper.hosts.list", "192.168.0.1" );
		if ( defaultServersList != null && !defaultServersList.isEmpty() ) {
			defaultServers = Arrays.asList( defaultServersList.split( "\\s*,\\s*" ) );
			logger.info( "zookeeper.hosts.list is {}", defaultServers );
		}
		if ( !useLocalServers && zkServersIps != null ) {
			hostnames = zkServersIps;
		}
		if ( hostnames.isEmpty() || useLocalServers ) {
			logger.error( "No zookeeper servers found in networks.json or running with useLocalServers - using defaule servers from conf file" );
			hostnames = defaultServers;
		}
		logger.info( "Zookeeper servers list is - {}", hostnames );
		StringBuilder sb = new StringBuilder();
		String delim = "";
		for ( String hostname : hostnames ) {
			sb.append( delim );
			sb.append( hostname );
			delim = ",";
		}
		return sb.toString();
	}

}
