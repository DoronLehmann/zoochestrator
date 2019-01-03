package com.imperva.zoochestrator.tasks.algo;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.imperva.zoochestrator.latch.CuratorMasterLatch;
import com.imperva.zoochestrator.latch.WorkerNodeData;
import com.imperva.zoochestrator.tasks.DistributionTask;
import com.imperva.zoochestrator.utils.BaseWorkerThread;
import com.imperva.zoochestrator.utils.ZookeeperUtils;



/**
 * This is an algorithm for handling the task by the machine load. <br> 
 * The load factor is defined by the class which inherit from this class
 * 
 * @author Doron Lehmann
 * 
 */
public abstract class MachineLoadTaskDistributionAlgorithm implements TaskDistributedAlgorithm {
	
	
	/**
	 * an implementation of thread worker for updating the load factor every X minutes as determined by the "load.factor.update.interval" property
	 */
	public class Updater extends BaseWorkerThread {

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public Updater(String name, int id) {
			super( name, id );
			setTimer( Long.parseLong( System.getProperty( "load.factor.update.interval", "1" ) ), TimeUnit.MINUTES, true, null, new TimerCallback() {
				@Override
				public void handle( Object ctx ) {
					update();
				}
			} );
		}

		@Override
		public Logger getLogger() {
			return logger;
		}
		
	}
	
	private static final Logger logger = LoggerFactory.getLogger( MachineLoadTaskDistributionAlgorithm.class );
	
	/**
	 * a const for the key name the algorithm use to add the load factor data to the worker extra data map
	 */
	private static final String LOAD_FACTOR_KEY = "LoadFactor";
	
	/**
	 * the updater worker for this instace of the algorithm
	 */
	public Updater updater;
	
	/**
	 * the curator client for interacting with zookeeper
	 */
	public CuratorFramework client;
	
	/**
	 * the base path for the workers path
	 */
	public String basePath;
	
	/**
	 * the specific machine worker id
	 */
	public String id;
	
	/**
	 * the name for the algorithm updater thread
	 */
	public String name;
	
	/**
	 * 
	 * @param name
	 * @param client
	 * @param basePath
	 * @param id
	 */
	public MachineLoadTaskDistributionAlgorithm(String name, CuratorFramework client, String basePath, String id) {
		this.updater = new Updater(name, 0);
		this.client = client;
		this.basePath = basePath;
		this.id = CuratorMasterLatch.generateWorkerId(id);
		this.name = name;
	}
	
	/**
	 * method that return the current machine load
	 * @return the current machine load
	 */
	public abstract double getCurrentLoad();
	
	@Override
	public String getTaskTargetWorker( DistributionTask task ) throws Exception {
		logger.debug("deciding on worker for task {}", task.toString());
		WorkerNodeData selectedWorker = null;
		String selectedWorkerId = null;
		List<WorkerNodeData> workers = getWorkers();
		double selectedWorkerLoadFactor = 0;
		for(WorkerNodeData worker : workers) {
			double workerExtraLoadFactor = 0;
			if(worker.extraData != null) {
				if(worker.extraData.containsKey(LOAD_FACTOR_KEY)) {
					workerExtraLoadFactor = (double)worker.extraData.get(LOAD_FACTOR_KEY);
				}
			}
			logger.debug("worker {} has load factor of {}", workerExtraLoadFactor);
			if(selectedWorker == null || workerExtraLoadFactor < selectedWorkerLoadFactor) {
				selectedWorker = worker;
				selectedWorkerLoadFactor = workerExtraLoadFactor;
			}
		}
		if ( selectedWorker != null ) {
			logger.debug( "selected worker is {} and has load factor of {}", selectedWorker, selectedWorkerLoadFactor );
			selectedWorkerId = CuratorMasterLatch.generateWorkerId( selectedWorker.id );
			logger.debug( "selected worker id for task {} is {} ({})", task.basePath, selectedWorkerId, selectedWorker.id );
		}
		return selectedWorkerId;
	}
	
	@Override
	public void init() {
		this.updater.start();
	}
	
	/**
	 * a method that update the worker node data load factor
	 */
	private void update() {
		try {
			WorkerNodeData workerData = getWorker(this.id);
			if(workerData != null) {
				workerData.extraData.put(LOAD_FACTOR_KEY, getCurrentLoad());
				client.setData().forPath( this.basePath + "/workers/" + this.id, ZookeeperUtils.toBytes( CuratorMasterLatch.gson.toJson( workerData ) ) );
			}
			else {
				logger.error("couldn't find worker {} while trying to update its load factor", this.id);
			}
		}
		catch(Exception e) {
			logger.error("error occurred while trying to update worker {}", this.id);
		}
	}
	
	/**
	 * a method that return all the workers nodes data
	 * @return a list of all the workers node data
	 */
	private List<WorkerNodeData> getWorkers() throws Exception {
		List<WorkerNodeData> workers = new LinkedList<WorkerNodeData>();
		List<String> workersIds = client.getChildren().forPath(this.basePath + "/workers");
		for(String workerId : workersIds) {
			byte[] nData = client.getData().forPath( this.basePath + "/workers/" + workerId );
			WorkerNodeData worker = CuratorMasterLatch.gson.fromJson( ZookeeperUtils.fromBytes(nData), WorkerNodeData.class );
			workers.add(worker);
		}
		return workers;
	}
	
	/**
	 * method that retrieve a worker node data
	 * @param workerId - the id of the worker we want to retrieve
	 * @return the worker node data if the worker exists and null if not
	 */
	private WorkerNodeData getWorker(String workerId) throws Exception {
		try {
			byte[] nData = client.getData().forPath( this.basePath + "/workers/" + workerId );
			return CuratorMasterLatch.gson.fromJson( ZookeeperUtils.fromBytes(nData), WorkerNodeData.class );
		}
		catch(Exception e) {
			return null;
		}
	}

}
