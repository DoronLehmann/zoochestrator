package com.imperva.zoochestrator.latch;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * A base implementation of a Curator based <code>LeaderLatchListener</code>
 * 
 * @author Doron Lehmann
 *
 */
public abstract class CuratorBaseLatch implements LeaderLatchListener {

	private static final Logger logger = LoggerFactory.getLogger( CuratorBaseLatch.class );

	public static final Gson gson;

	public CuratorFramework client;

	public String id;
	public String basePath;

	public PathChildrenCache masterCache;
	public LeaderLatch leaderLatch;
	public TreeCacheListener globalListener;
	public TreeCache globalScopeCache;

	public InterProcessSemaphoreMutex nodesChangeLock;
	
	public int nodeChangeLockAcquireTime = Integer.parseInt( System.getProperty( "zookeeper.curator.node.change.lock.acquire.time.in.seconds", "60" ) );
	public int nodeChangeLockSleepTime = Integer.parseInt( System.getProperty( "zookeeper.curator.node.change.lock.sleep.time.in.seconds", "1" ) );
	public int maxNumberOfTriesForLock = Integer.parseInt( System.getProperty( "zookeeper.curator.node.change.lock.max.num.of.tries", "5" ) );

	static {
		gson = new Gson();
	}
	
	/**
	 * Close the master latch
	 */
	public abstract void close();

	/**
	 * 
	 * @throws Exception
	 */
	public void runForMaster() throws Exception {
		/*
		 * monitoring cache
		 */
		if ( Boolean.valueOf( System.getProperty( "zookeeper.debug.curatorMasterLatch", Boolean.TRUE.toString() ) ) ) {
			initGlobalListener();
			globalScopeCache = new TreeCache( client, basePath );
			globalScopeCache.getListenable().addListener( globalListener );
			globalScopeCache.start();
		}

		/*
		 * Start master election
		 */
		logger.info( "Starting master selection: {}", id );
		leaderLatch.addListener( this );
		leaderLatch.start();
	}

	/**
	 * Checks if the current system has the leadership for this scope
	 * @return true if it has the leadership, otherwise false
	 */
	public boolean hasLeadership() {
		return leaderLatch.hasLeadership();
	}

	/**
	 * Tries to acquire the lock in order to either register to the scope or to reassign the scope tasks
	 * @return true if the lock is acquired 
	 * @throws Exception
	 */
	public boolean acquireLock() {
		int tries = 0;
		boolean result = false;
		logger.info( "Trying to aquired the nodesChangeLock for scope {}", basePath );
		// we try to acquire the lock for a max number of tries
		while ( tries < maxNumberOfTriesForLock ) {
			boolean locked = false;
			boolean hasException = false;
			try {
				locked = nodesChangeLock.acquire( nodeChangeLockAcquireTime, TimeUnit.SECONDS );
				if ( locked ) {
					logger.info( "Successfully aquired the nodesChangeLock for scope {}", basePath );
					result = true;
					break;
				}
			}
			catch ( Exception e ) {
				// This can happen in cases of ZK errors or connection interruptions
				hasException = true;
			}
			if ( hasException ) {
				logger.error( "An error related to ZK occurd while trying to fetch the nodesChangeLock for scope {}. Sleeping for {} seconds - try number {} out of {}", basePath, nodeChangeLockSleepTime, tries, maxNumberOfTriesForLock );
			}
			else {
				//failed to acquire the lock in the given time frame
				logger.error( "Failed to fetch the nodesChangeLock for scope {} in the given timeframe of {} seconds. Sleeping for {} seconds - try number {} out of {}", basePath, nodeChangeLockAcquireTime, nodeChangeLockSleepTime, tries, maxNumberOfTriesForLock );
			}
			try {
				Thread.sleep( nodeChangeLockSleepTime * 1000 );
				logger.error( "Slept for {} seconds for scope {}", nodeChangeLockSleepTime, basePath );
			}
			catch ( InterruptedException e ) {
				logger.error( "Failed to sleep for {} seconds", nodeChangeLockSleepTime, e );
			}
			tries++;
		}
		if ( !result ) {
			logger.error( "Failed to fetch the nodesChangeLock for scope {} after {} tries", basePath, tries );
		}
		return result;
	}

	/**
	 * Releases the lock
	 * @throws Exception
	 */
	public void releaseLock() {
		logger.info( "Releasing the nodesChangeLock for scope {}", basePath );
		try {
			nodesChangeLock.release();
			logger.info( "Released the nodesChangeLock for scope {}", basePath );
		}
		catch ( Exception e ) {
			logger.error( "Exception when trying to release the nodesChangeLock for scope {}", basePath, e );
		}
	}
	

	/*
	 * we add one global listener that listen to all event under the scope ( this.basedir ).
	 * this listener can help in monitoring this CuratorMasterCache instance.
	 */
	public void initGlobalListener(){
		globalListener = new TreeCacheListener() {
			@Override
			public void childEvent( CuratorFramework client, TreeCacheEvent event ) throws Exception {
				String path = null;
				ChildData childData = event.getData();
				if ( childData != null ) {
					path = childData.getPath();
				}
				switch ( event.getType() ) {
					case CONNECTION_LOST:
						logger.debug( "event: CONNECTION_LOST" );
						break;
					case CONNECTION_RECONNECTED:
						logger.debug( "event: CONNECTION_RECONNECTED" );
						break;
					case CONNECTION_SUSPENDED:
						logger.debug( "event: CONNECTION_SUSPENDED" );
						break;
					case INITIALIZED:
						logger.debug( "scope cache INITIALIZED for scope  {}", basePath );
						break;
					case NODE_ADDED:
						if ( path != null && path.contains( "/assign" ) ) {
							logger.debug( "Task assigned correctly for path: {}", path );
						}
						else if ( path != null && path.contains( "/tasks" ) ) {
							logger.debug( "new Task added for path: {}", path );
						}
						else if ( path != null && path.contains( "/workers" ) ) {
							logger.debug( "worker added for path: {}", path );
						}
						else if ( path != null && path.contains( "/master" ) ) {
							logger.debug( "master added for path: {}", path );
						}
						else {
							logger.debug( "NODE_ADDED for path {}", path );
						}
						break;
					case NODE_REMOVED:
						if ( path != null && path.contains( "/tasks" ) ) {
							logger.debug( "Task correctly deleted from the /tasks/ node: {}", path );
						}
						else if ( path != null && path.contains( "/assign" ) ) {
							logger.debug( "Task correctly deleted from the /assign/ node: {}", path );
							break;
						}
						else if ( path != null && path.contains( "/workers" ) ) {
							logger.debug( "worker removed for path: {}", path );
						}
						else if ( path != null && path.contains( "/master" ) ) {
							logger.debug( "master removed for path: {}", path );
						}
						else {
							logger.debug( "NODE_REMOVED for path {}", path );
						}
						break;
					case NODE_UPDATED:
						logger.debug( "NODE_UPDATED for path {}", path );
						break;
					default:
						break;
				}
			}
		};
	}

}
