package com.imperva.zoochestrator.tasks;

import java.util.Date;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;


/**
 * Use this class if you want to create task that will be executed after <code>executionTime</code>.
 * The <code>ScheduledTask</code> will become pending task after the <code>executionTime</code>.
 * 
 * @author Alon Resler
 *
 */
public class ScheduledTask extends Task {
	
	public String executionTime;

	public ScheduledTask(String data, String path, Date executionTime) {
		super(data, path);
		this.executionTime = String.valueOf( executionTime.getTime() );
	}
	
	public ScheduledTask(PathChildrenCacheEvent event, String data, Date executionTime) {
		super(event, data);
		this.executionTime = String.valueOf( executionTime.getTime() );
	}

	
}
