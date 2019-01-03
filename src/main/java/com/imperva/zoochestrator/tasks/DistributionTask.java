package com.imperva.zoochestrator.tasks;

import java.util.List;

/**
 * 
 * @author Doron Lehmann
 *
 */
public class DistributionTask {
	
	public String basePath;
	public String taskPath;
	public List<String> workers;
	
	public DistributionTask( String basePath, String taskPath, List<String> workers ) {
		this.basePath = basePath;
		this.taskPath = taskPath;
		this.workers = workers;
	}

}
