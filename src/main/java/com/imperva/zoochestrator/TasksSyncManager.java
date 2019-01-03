package com.imperva.zoochestrator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.imperva.zoochestrator.callbacks.ZooKeeperChildrenEventCallback;
import com.imperva.zoochestrator.latch.CuratorMasterLatch;
import com.imperva.zoochestrator.tasks.ScheduledTask;
import com.imperva.zoochestrator.tasks.Task;
import com.imperva.zoochestrator.tasks.TaskAssignmentCallback;
import com.imperva.zoochestrator.tasks.algo.TaskDistributedAlgorithm;
import com.imperva.zoochestrator.utils.BaseWorkerThread;
import com.imperva.zoochestrator.utils.ZookeeperUtils;


/**
 * 
 * @author Doron Lehmann
 *
 */
public class TasksSyncManager {

	/**
	 * Number of tasks that are waiting to be scheduled
	 */
	public static final String SCHEDULED_TASKS = "scheduledTasks";
	
	/**
	 * Number of tasks that are waiting to be assigned
	 */
	public static final String UNASSIGNED_TASKS = "unassignedTasks";
	
	/**
	 * Total number of tasks that are pending execution
	 */
	public static final String PENDING_TASKS = "pendingTasks";

	private static final Logger logger = LoggerFactory.getLogger( TasksSyncManager.class );
	
	public CuratorFramework client;
	private Map<String, CuratorMasterLatch> masterLatchMap;
	private String id;
	private HouseKeeperWorker worker;
	
	/**
	 * A thread pool for managing the registration of the master latch 
	 */
	private ExecutorService zkExPool = null;
	private Map<String, Future<?>> scopeRegistrationMap;

	
	/**
	 * Creates a new Tasks sync manager
	 */
	public TasksSyncManager() {
		masterLatchMap = new ConcurrentHashMap<String, CuratorMasterLatch>();
		scopeRegistrationMap = new ConcurrentHashMap<String, Future<?>>();
		worker = new HouseKeeperWorker();
		zkExPool = Executors.newFixedThreadPool( Integer.parseInt( System.getProperty( "task.sync.manager.zk.threadpool.size", "10" ) ) );
	}

	/**
	 * Initialize the manager.<br>
	 * Each manager should be closed by calling the close() method once it is no longer in use.
	 * @param zkServersIps
	 * @param id the id of the application which inits the manager
	 * @param namespace  used to set a prefix - mainly for dev and test environment. Can be null, can't be 'zookeeper'
	 * @param username for the distributed system
	 * @param password for the distributed system
	 * @param useLocalServers determines if to ignore the zkServersIps and connect to what is defined in "zookeeper.hosts.list"
	 * @throws Exception if failed to init the tasks sync manager
	 */
	public void init( List<String> zkServersIps, String id, String namespace, String username, String password, boolean useLocalServers ) throws Exception {
		logger.info( "Initializing the Distributed Sync Manager..." );
		CuratorFramework curatorFrameworkClient = ZookeeperService.createAndStartClient( zkServersIps, namespace, username, password, useLocalServers );
		setAndStart( curatorFrameworkClient, id );
		logger.info( "Initilized the Distributed Sync Manager" );
	}
	
	/**
	 * Initialize the manager with the given CuratorFramework client.<br>
	 * Each manager should be closed by calling the close() method once it is no longer in use.
	 * @param client CuratorFramework client instance
	 * @param id the id of the application which inits the manager
	 * @throws Exception if failed to init the tasks sync manager
	 */
	public void init( CuratorFramework client, String id ) throws Exception {
		logger.info( "Initializing the Distributed Sync Manager..." );
		setAndStart( client, id );
		logger.info( "Initilized the Distributed Sync Manager" );
	}
	
	private void setAndStart( CuratorFramework curatorFrameworkClient, String id ) throws Exception {
		if ( curatorFrameworkClient != null ) {
			this.client = curatorFrameworkClient;
			this.id = id;
			this.worker.start();
		}
		else {
			String message = "Failed to create and start the CuratorFramework client - the CuratorFramework instance is null";
			logger.error( message );
			throw new Exception( message );
		}
	}

	/**
	 * Closes all the relevant connections of the manager
	 */
	public void close() {
		logger.info( "Closing the Distributed Sync Manager..." );
		worker.baseStop();
		worker.baseJoin();
		for ( CuratorMasterLatch masterLatch : masterLatchMap.values() ) {
			masterLatch.close();
		}
		ZookeeperService.closeClient( client );
		zkExPool.shutdown();
		logger.info( "Closed the Distributed Sync Manager" );

	}

	/**
	 * Registers to the provided scope<br>
	 * Registered instances will select a master node which will manage the registered instances tasks
	 * @param scope the scope to register to - must start with a backslash "/"
	 * @param runForMaster true if the manager should run for master role, <br> false for workers scenario which do not want to acquire leadership
	 * @param taskDistributionAlgorithm the implementation for distributing the tasks. If null is passed, The random algorithm will be used.
	 * @param callback the callback that should be executed on task assignment
	 * @param workerNodeExtraData to be set on the registered system worker node
	 * @throws Exception
	 */
	public void register( final String scope, final boolean runForMaster, final TaskDistributedAlgorithm taskDistributionAlgorithm, final TaskAssignmentCallback callback, final Map<String, Object> workerNodeExtraData) throws Exception {
		logger.info( "Registering to the Distributed Sync Manager - scope {} ", scope );
		scopeRegistrationMap.put( scope, zkExPool.submit( new Runnable() {
			@Override
			public void run() {
				try {
					logger.info( "Calling Create And Start MasterLatch for scope {}", scope );
					CuratorMasterLatch latch = ZookeeperService.createAndStartMasterLatch( client, id, scope, taskDistributionAlgorithm, runForMaster, workerNodeExtraData );
					logger.info( "Adding the MasterLatch for scope {} to the masterLatchMap", scope );
					masterLatchMap.put( scope, latch );
					ZookeeperService.createAndAddTasksAssignmentListener( client, id, scope, new ZooKeeperChildrenEventCallback() {
						@Override
						public void execute( PathChildrenCacheEvent event ) throws Exception {
							Task task = new Task( event, ZookeeperUtils.fromBytes( client.getData().forPath( event.getData().getPath() ) ) );
							logger.debug( "Got a new task - {} ", task );
							callback.execute( task );
						}
					} );
				}
				catch ( Exception e ) {
					logger.error( "Failed to register to the Distributed Sync Manager - scope {} ", scope, e );
				}
			}
		} ));
	}

	/**
	 * Registers to the provided scope<br>
	 * Registered instances will select a master node which will manage the registered instances tasks
	 * @param scope the scope to register to - must start with a backslash "/"
	 * @param runForMaster true if the manager should run for master role, <br> false for workers scenario which do not want to acquire leadership
	 * @param taskDistributionAlgorithm the implementation for distributing the tasks. If null is passed, The random algorithm will be used.
	 * @param callback the callback that should be executed on task assignment
	 * @throws Exception
	 */
	public void register( final String scope, final boolean runForMaster, final TaskDistributedAlgorithm taskDistributionAlgorithm, final TaskAssignmentCallback callback) throws Exception {
		register( scope, runForMaster, taskDistributionAlgorithm, callback, new HashMap<String, Object>() );
	}
	
	/**
	 * Registers to the provided scope<br>
	 * Registered instances will select a master node which will manage the registered instances tasks
	 * @param scope the scope to register to - must start with a backslash "/"
	 * @param runForMaster true if the manager should run for master role, <br> false for workers scenario which do not want to acquire leadership
	 * @param callback the callback that should be executed on task assignment
	 * @throws Exception
	 */
	public void register( String scope, boolean runForMaster, final TaskAssignmentCallback callback) throws Exception {
		register( scope, runForMaster, null, callback );
	}
	
	/**
	 * Creates a task
	 * @param scope the scope to add the task to - must start with a backslash "/"
	 * @param task the task
	 * @throws Exception if task creation fails
	 */
	public void createTask( String scope, Task task ) throws Exception {
		logger.debug( "Creating task for {} for scope {}", task, scope );
		CuratorMasterLatch scopeCuratorMasterLatch = getMasterLatch( scope );
		String metadata = task.getMetadata() != null ? "-" + task.getMetadata() : "";
		String path = scopeCuratorMasterLatch.basePath + "/tasks/task" + metadata + "-";
		client.create().withProtection().withMode( CreateMode.PERSISTENT_SEQUENTIAL ).inBackground().forPath( path, ZookeeperUtils.toBytes( task.data ) );
	}
	
	/**
	 * Creates a config node for the first time plus its first task
	 * @param path the path of the config node
	 * @param data the data for the config node
	 * @param scope the scope for the task
	 * @param task the task to add
	 * @throws Exception 
	 */
	public void createFirstConfigAndTask( String path, byte[] data, String scope, Task task ) throws Exception {
		logger.debug( "Creating first time config node and task for {} for scope {} and task {} ...", path, scope, task );
		CuratorMasterLatch scopeCuratorMasterLatch = getMasterLatch( scope );

		String metadata = task.getMetadata() != null ? "-" + task.getMetadata() : "";
		String taskPath = scopeCuratorMasterLatch.basePath + "/tasks/task" + metadata + "-";
		String pathToEnsure = path.substring( 0, path.lastIndexOf( "/" ) );
		client.newNamespaceAwareEnsurePath( pathToEnsure ).ensure( client.getZookeeperClient() );
		logger.debug( "ensured path of {}", pathToEnsure );
		client.inTransaction().
		create().forPath( path, data ).and().
		create().withMode( CreateMode.PERSISTENT_SEQUENTIAL ).forPath( taskPath, ZookeeperUtils.toBytes( task.data ) ).and().
		commit();
		logger.debug( "Created the first time config node and task for {} for scope {} and task {}", path, scope, task );
	}
	
	/**
	 * Create a task with execution time. Only guarantee is that the task will be executed after the timeToExecute <br>
	 * implemented by working thread that wakes up every 10 seconds and transfer the task to "runnable" state
	 * @param scope - the scope to add the task to - must start with a backslash "/"
	 * @param task - the task to execute 
	 * @param timeToExecute - milliseconds since the epoch. The task will be executed after this time. 
	 * @throws Exception
	 */
	public void createScheduledTask( String scope, ScheduledTask task, Long timeToExecute ) throws Exception {
		logger.debug( "Creating scheduled task for {} for scope {}", task, scope );
		CuratorMasterLatch scopeCuratorMasterLatch = getMasterLatch( scope );
		String path = scopeCuratorMasterLatch.basePath + "/scheduledTasks/" + timeToExecute +"/task-";
		client.create().creatingParentsIfNeeded().withProtection().withMode( CreateMode.PERSISTENT_SEQUENTIAL ).inBackground().forPath( path, ZookeeperUtils.toBytes( task.data ) );
	}
	
	/**
	 * Deletes a task.<br>
	 * NOTE - If you intent to delete a task and then add a following task, please use {@link deleteAndCreateFollowingTask}
	 * @param task the task to delete
	 * @throws Exception if task deletion fails
	 */
	public void deleteTask( Task task ) throws Exception {
		String path = task.path;
		logger.debug( "Deleting task: {}", task );
		client.delete().guaranteed().forPath( path );
	}
	
	/**
	 * Deletes a task and creates a following task in the same transaction.
	 * @param scope the scope to add the task to - must start with a backslash "/"
	 * @param taskToDelete the task to delete
	 * @param taskToAdd the next task data
	 * @throws Exception if the action fails
	 */
	public void deleteAndCreateFollowingTask( String scope, Task taskToDelete, Task taskToAdd ) throws Exception {
		logger.debug( "Deleting task: {} and creating task for {}", taskToDelete, taskToAdd );
		CuratorMasterLatch scopeCuratorMasterLatch = getMasterLatch( scope );
		String taskToDeletePath = taskToDelete.path;
		String metadata = taskToAdd.getMetadata() != null ? "-" + taskToAdd.getMetadata() : "";
		String taskToAddPath = scopeCuratorMasterLatch.basePath + "/tasks/task" + metadata + "-";
		//we perform the deletion and creation in one transaction
		client.inTransaction().
			delete().forPath( taskToDeletePath ).
			and().
			create().withMode( CreateMode.PERSISTENT_SEQUENTIAL ).forPath( taskToAddPath, ZookeeperUtils.toBytes( taskToAdd.data ) ).
			and().
			commit();
	}

	/**
	 * Whether or not this process is a leader of the specified scope
	 * @param scope the scope
	 * @throws Exception 
	 */
	public boolean hasLeadership( String scope ) throws Exception {
		boolean result = getMasterLatch( scope ).hasLeadership();
		return result;
	}
	
	
	/**
	 * Gets the status of the tasks in the system for a given scope
	 * @param scope the scope to get the tasks status for
	 * @return a <code>Map<String, Integer></code> of zNodes in the system and the number of tasks that they contain 
	 * @throws Exception
	 */
	public Map<String, Integer> getTasksStatus( String scope ) throws Exception {
		logger.debug( "Geting tasks status for scope {}", scope );
		CuratorMasterLatch scopeCuratorMasterLatch = getMasterLatch( scope );
		Map<String, Integer> tasksStatusMap = new HashMap<String, Integer>();
		int pendingTasks = 0;
		String tasksPath = scopeCuratorMasterLatch.basePath + "/tasks";
		String scheduledTasksPath = scopeCuratorMasterLatch.basePath + "/scheduledTasks";
		String assignPath = scopeCuratorMasterLatch.basePath + "/assign";
		
		//get the unassigned tasks
		List<String> tasksChildren = client.getChildren().forPath( tasksPath );
		if ( tasksChildren != null ) {
			tasksStatusMap.put( UNASSIGNED_TASKS, tasksChildren.size() );
			pendingTasks += tasksChildren.size();
		}
		else{
			logger.debug( "no unassigned tasks for scope {}", scope );
		}
		List<String> scheduledTasksChildren = client.getChildren().forPath( scheduledTasksPath );
		if ( scheduledTasksChildren != null ) {
			tasksStatusMap.put( SCHEDULED_TASKS, scheduledTasksChildren.size() );
			pendingTasks += scheduledTasksChildren.size();
		}
		else{
			logger.debug( "no unassigned scheduled tasks for scope {}", scope );
		}
		//get the assigned tasks
		List<String> assignChildren = client.getChildren().forPath( assignPath );
		for ( String assignChildPath : assignChildren ) {
			List<String> assignChildChildren = client.getChildren().forPath( assignPath +"/"+ assignChildPath );
			if ( assignChildChildren != null ) {
				tasksStatusMap.put( assignChildPath, assignChildChildren.size() );
				pendingTasks += assignChildChildren.size();
			}
			else{
				logger.debug( "no unassigned tasks for assignd node {} for scope {}", assignChildPath, scope );
			}
		}
		tasksStatusMap.put( PENDING_TASKS, pendingTasks );
		return tasksStatusMap;
	}

	/**
	 * Gets all the assigned tasks and their creation time
	 * @param scope the scope to get the tasks status for
	 * @return a <code>Map<String, Long></code> of zNodes in the system and their creation time 
	 * @throws Exception
	 */
	public Map<String, Long> getAssignedTasksStatus( String scope ) throws Exception {
		logger.debug( "Geting assigned tasks status for scope {}", scope );
		CuratorMasterLatch scopeCuratorMasterLatch = getMasterLatch( scope );
		Map<String, Long> assignedTasksStatusMap = new HashMap<String, Long>();
		String assignPath = scopeCuratorMasterLatch.basePath + "/assign";
		// get the assigned tasks
		List<String> assignChildren = client.getChildren().forPath( assignPath );
		for ( String assignChildPath : assignChildren ) {
			List<String> assignChildChildren = client.getChildren().forPath( assignPath + "/" + assignChildPath );
			for ( String taskPath : assignChildChildren ) {
				String fullTaskPath = assignPath + "/" + assignChildPath + "/" + taskPath;
				Stat taskStat = client.checkExists().forPath( fullTaskPath );
				if ( taskStat != null ) {
					long ctime = taskStat.getCtime();
					assignedTasksStatusMap.put( fullTaskPath, ctime );
				}
			}
		}
		return assignedTasksStatusMap;
	}
	
	/**
	 * Gets a snapshot of of the tasks in the system for a given scope
	 * @param scope the scope to get the tasks status for
	 * @return a <code>Map<String, List<String>></code> of zNodes in the system and the tasks that they contain 
	 * @throws Exception
	 */
	public Map<String, List<String>> getTasksInfo( String scope ) throws Exception {
        if (scope == null) {
            logger.error( "Can't get tasks, scope is null");
            throw new IllegalArgumentException();
        }

        logger.debug( "Geting tasks status for scope {}", scope );
		Map<String, List<String>> tasksStatusMap = new HashMap<String, List<String>>();
		String tasksPath = scope + "/tasks";
		String scheduledTasksPath = scope + "/scheduledTasks";
		String assignPath = scope + "/assign";

		// get the unassigned tasks
		if ( client.checkExists().forPath( tasksPath ) != null ) {
			List<String> tasksChildren = client.getChildren().forPath( tasksPath );
			if ( tasksChildren != null ) {
				tasksStatusMap.put( UNASSIGNED_TASKS, new ArrayList<String>( tasksChildren.size() ) );
				for ( String currTaskPath : tasksChildren ) {
					try{
						byte[] taskData = client.getData().forPath( tasksPath + "/" + currTaskPath );
						if ( taskData != null ) {
							tasksStatusMap.get( UNASSIGNED_TASKS ).add( ZookeeperUtils.fromBytes( taskData ) );
						}
					}
					catch ( Exception e ) {
						logger.debug( "Error while trying to get unassigned task info for scope {}, path {}", scope, tasksPath, e );
					}
				}
			}
			else {
				logger.debug( "no unassigned tasks for scope {}", scope );
			}
		}
		if ( client.checkExists().forPath( scheduledTasksPath ) != null ) {
			List<String> scheduledTasksChildren = client.getChildren().forPath( scheduledTasksPath );
			if ( scheduledTasksChildren != null ) {
				tasksStatusMap.put( SCHEDULED_TASKS, new ArrayList<String>( scheduledTasksChildren.size() ) );
				for ( String currTaskPath : scheduledTasksChildren ) {
					try{
						byte[] taskData = client.getData().forPath( scheduledTasksPath + "/" + currTaskPath );
						if ( taskData != null ) {
							tasksStatusMap.get( SCHEDULED_TASKS ).add( ZookeeperUtils.fromBytes( taskData ) );
						}
					}
					catch ( Exception e ) {
						logger.debug( "Error while trying to get scheduled task info for scope {}, path {}", scope, tasksPath, e );
					}
				}
			}
			else {
				logger.debug( "no unassigned scheduled tasks for scope {}", scope );
			}
		}
		// get the assigned tasks
		if ( client.checkExists().forPath( assignPath ) != null ) {
			List<String> assignChildren = client.getChildren().forPath( assignPath );
			for ( String assignChildPath : assignChildren ) {
				List<String> assignChildChildren = client.getChildren().forPath( assignPath + "/" + assignChildPath );
				if ( assignChildChildren != null ) {
					tasksStatusMap.put( assignChildPath, new ArrayList<String>( assignChildChildren.size() ) );
					for ( String currTaskPath : assignChildChildren ) {
						String currTaskFullPath = assignPath + "/" + assignChildPath + "/" + currTaskPath;
						try {
							byte[] taskData = client.getData().forPath( currTaskFullPath );
							if ( taskData != null ) {
								tasksStatusMap.get( assignChildPath ).add( ZookeeperUtils.fromBytes( taskData ) );
							}
						}
						catch ( Exception e ) {
							logger.debug( "tried to get data for task {} - but the task seems to no longer exists - exception is {}", currTaskFullPath, e.getMessage() );
						}
					}
				}
				else {
					logger.debug( "no unassigned tasks for assignd node {} for scope {}", assignChildPath, scope );
				}
			}
		}
		return tasksStatusMap;
	}
	

    /**
     * Attempts to force assign the given task to a worker.
     * @param task The task to force assign to the given target worker
     * @throws Exception If parsing of task fails or if the latch used throws an exception
     */
    public void forceAssignTask(String task) throws Exception {
        String taskScope = ZookeeperUtils.extractScope(task);
        String taskNode = task.replaceFirst( taskScope + "/tasks/", "");
        CuratorMasterLatch latch = getMasterLatch(taskScope);
        latch.forceAssignTask(taskNode, null);
    }

    /**
     * Attempts to force assign the given task to the given worker.<br>
     * The task and worker must both exist and be in the same scope.
     * @param task The task to force assign to the given target worker
     * @param targetWorker The worker to force assign the given task to
     * @throws Exception If parsing of task and worker fails, if their scopes are non-identical, or if the latch used throws an exception
     */
	public void forceAssignTask(String task, String targetWorker) throws Exception {
        String taskScope = ZookeeperUtils.extractScope(task);
        String targetWorkerScope = ZookeeperUtils.extractScope(targetWorker);

        if (!taskScope.equals(targetWorkerScope)) {
            final String errorMessage = "Cannot force assign task " + task + " to worker " + targetWorker + " - different scopes are not allowed!";
            logger.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }

        String taskNode = task.replaceFirst( taskScope + "/tasks/", "");
        String targetWorkerNode = targetWorker.replaceFirst( targetWorkerScope + "/workers/", "");

        CuratorMasterLatch latch = getMasterLatch(taskScope);
        latch.forceAssignTask(taskNode, targetWorkerNode);
    }
	
	/**
	 * Gets the CuratorMasterLatch. Each method which needs to use the master latch must call this method in order to get it.
	 * @param scope
	 * @return the CuratorMasterLatch
	 * @throws Exception
	 */
	private CuratorMasterLatch getMasterLatch( String scope ) throws Exception {
		getMasterLatchFromFuture( scope );
		CuratorMasterLatch scopeCuratorMasterLatch = masterLatchMap.get( scope );
		if ( scopeCuratorMasterLatch == null ) {
			throw new IllegalArgumentException( "No latch for scope " + scope );
		}
		return scopeCuratorMasterLatch;
	}
	
	private void getMasterLatchFromFuture( String scope ) throws Exception {
		Future<?> masterLatchFuture = scopeRegistrationMap.get( scope );
		if ( masterLatchFuture == null ) {
			throw new IllegalArgumentException( "No future latch for scope " + scope );
		}
		if ( !masterLatchFuture.isDone() ) {
			try {
				masterLatchFuture.get( Long.parseLong( System.getProperty( "task.sync.manager.future.get.timeout", "300" ) ), TimeUnit.SECONDS );
			}
			catch ( Exception e ) {
				logger.error( "Error while trying to check the registration state for scope {}", scope );
				throw e;
			}
		}
	}
	
	private class HouseKeeperWorker extends BaseWorkerThread {

		public HouseKeeperWorker() {
			super( "tasks_sync_manager_house_keeper_worker", 0 );
			setTimer( Long.parseLong( System.getProperty( "task.sync.manager.worker.time", "10" ) ), TimeUnit.SECONDS, true, null, new TimerCallback<Void>() {
				@Override
				public void handle( Void ctx ) {
					for ( String scope : masterLatchMap.keySet() ) {
						try {
							CuratorMasterLatch latch = getMasterLatch(scope);
							if ( latch.hasLeadership() ) {
								latch.periodicScheduledTaskCheck();
							}
						}
						catch ( Exception e ) {
							logger.error( "error while trying to get the master latch");
						}
					}
					for ( CuratorMasterLatch latch : masterLatchMap.values() ) {
						if ( latch.hasLeadership() ) {
							latch.periodicScheduledTaskCheck();
						}
					}
				}
			} );
		}

		@Override
		public Logger getLogger() {
			return logger;
		}

		@Override
		public void loop() {
			while ( running ) {
				checkPoint( "Starting loop");
				if ( !suspended ) {
					triggerTimers();
				}
				baseSleep( 1L, TimeUnit.SECONDS );
			}
		}
	}
	
}
