package com.imperva.zoochestrator.utils;

public interface SelfMonitoringThread {
	
	public String getName();
	
	public boolean isStuck();
	
	/**
	 * Hook for implementing classes when the thread is detected to be stuck
	 * @param counter the number of consecutive times that the thread has been detected to be stuck. For example, if stuckTimeThresholdMillis equals 10 minutes, and the monitoring thread 
	 * runs every 1 minutes - the method will be called for the first time after 10 minutes with counter = 1, the next minute with counter = 2 and so on.
	 * If the thread is freed up the counter is zeroed.
	 */
	public void onStuckDetectionCallback( int counter );
	
	/**
	 * Hook for implementing classes when the thread is detected to be get freed up from being stuck
	 */
	public void onFreedUpCallback();
}
