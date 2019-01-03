package com.imperva.zoochestrator.callbacks;

/**
 * 
 * @author Doron Lehmann
 *
 */
public interface LeadershipStatusChangeCallback {

	/**
	 * Will be called on leadership status change
	 * @param isLeader true if the status is now changed to become a leader
	 */
	public void execute( boolean isLeader );

}
