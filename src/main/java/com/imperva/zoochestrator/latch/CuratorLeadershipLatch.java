package com.imperva.zoochestrator.latch;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.imperva.zoochestrator.callbacks.LeadershipStatusChangeCallback;

/**
 * 
 * @author Doron Lehmann
 *
 */
public class CuratorLeadershipLatch extends CuratorBaseLatch {

	private static final Logger logger = LoggerFactory.getLogger( CuratorLeadershipLatch.class );
	
	private LeadershipStatusChangeCallback leaderCallback;

	/**
	 * Creates a new CuratorLeadershipLatch
	 * @param client CuratorFramework ZooKeeper client
	 * @param id the identifier
	 * @param path the base path, must start with a backslash "/"
	 * @param callback a callback when a leadership has been changed
	 * @throws IllegalArgumentException
	 * @throws Exception
	 */
	public CuratorLeadershipLatch( CuratorFramework client, String id, String path, LeadershipStatusChangeCallback callback  ) throws IllegalArgumentException, Exception {
		if ( client != null && id != null && !id.isEmpty() && path != null && !path.isEmpty() && path.startsWith( "/" ) ) {
			this.client = client;
			this.id = id;
			this.basePath = path;
			this.leaderCallback = callback;
			this.client.newNamespaceAwareEnsurePath( this.basePath + "/nodes_change_lock" ).ensure( client.getZookeeperClient() );
			// we need a lock in order to prevent assign and worker nodes change simultaneously
			nodesChangeLock = new InterProcessSemaphoreMutex( client, this.basePath + "/nodes_change_lock" );
			if ( !acquireLock() ) {
				String msg = "Failed to equire lock in order to start the CuratorLeadershipLatch for scope " + basePath;
				logger.error( msg );
				throw new Exception( msg );
			}
			try {
				this.leaderLatch = new LeaderLatch( this.client, this.basePath + "/master", id );
				this.masterCache = new PathChildrenCache( this.client, this.basePath + "/master", true );
				logger.info( "Running for master - adding listenerm starting the master cache and calling runForMaster" );
				runForMaster();
				logger.info( "Done running the Running for master logic" );
			}
			finally {
				// critical to release the lock
				releaseLock();
			}
		}
		else {
			throw new IllegalArgumentException( "One of the passed parames is invalid. Please see documentation for reference." );
		}
	}
	
	/**
	 * Executed when client becomes leader.
	 */
	@Override
	public void isLeader() {
		logger.info( "I am the leader: {}", id );
		leaderCallback.execute( true );
	}

	@Override
	public void notLeader() {
		logger.info( "Lost leadership - I am {} ", id );
		leaderCallback.execute( false );
	}

	@Override
	public void close() {
		logger.info( "Closing the {} master latch and closing all of its cache nodes", basePath );
		try {
			if ( masterCache != null ) {
				masterCache.close();
			}
			if ( globalScopeCache != null ) {
				globalScopeCache.close();
			}
			if ( leaderLatch != null ) {
				leaderLatch.close();
			}
		}
		catch ( Exception e ) {
			logger.error( "Failed to close cache node", e );
		}
		finally {
			if ( nodesChangeLock.isAcquiredInThisProcess() ) {
				releaseLock();
			}
		}
	}

}