package com.imperva.zoochestrator.callbacks;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

/**
 * An interface for a callback in the master/workers paradigm
 * 
 * @author Doron Lehmann
 *
 */
public interface ZooKeeperChildrenEventCallback {

	/**
	 * Will be called on a child event (i.e. task assignment)
	 * @param event
	 */
	public void execute( PathChildrenCacheEvent event ) throws Exception;

}
