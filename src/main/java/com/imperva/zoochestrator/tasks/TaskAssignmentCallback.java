package com.imperva.zoochestrator.tasks;

/**
 * 
 * @author Doron Lehmann
 *
 */
public interface TaskAssignmentCallback {
	
	/**
	 * Will be called on task assignment
	 * @param task
	 */
	public void execute( Task task ) ;


}
