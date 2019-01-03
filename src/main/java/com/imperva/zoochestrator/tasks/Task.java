package com.imperva.zoochestrator.tasks;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

/**
 * 
 * @author Doron Lehmann
 *
 */
public class Task {
	
	public String data;
	public PathChildrenCacheEvent event;
	public String path;
	private String metadata;
	private static final String invalidExp = "[^a-zA-Z0-9-_]";
	private static final String METADATA_DIVIDER = "-";
	private static final String METADATA_KEY_VAL_DIVIDER = "_";
	
	public Task ( String data, String path ) {
		this.data = data;
		this.path = path;
	}
	
	public Task ( PathChildrenCacheEvent event, String data){
		this.event = event;
		this.data = data;
		this.path = event.getData().getPath();
	}
	
	public String getMetadata() {
		return metadata;
	}
	
	/**
	 * Setting the meta data for the task. this will be a part of the task path in ZK.<br>
	 * For example - setting <b>"accountId_123-configId_456"</b> will create a task path of <b>/tasks/task-accountId_123-configId_456</b> <br>
	 * this could optimize unnecessary calls for getting the data of the task when all is needed is just the metadata.<br>.	
	 * @param metadataMap
	 */
	public void setMetadata( Map<String, String> metadataMap ) {
		if ( metadataMap != null ) {
			StringBuilder sb = new StringBuilder();
			for ( Entry<String, String> entry : metadataMap.entrySet() ) {
				sb.append( entry.getKey() ).append( METADATA_KEY_VAL_DIVIDER ).append( entry.getValue() ).append( METADATA_DIVIDER );
			}
			if ( sb.length() > 0 ) {
				sb.setLength( sb.length() - 1 );
			}
			this.metadata = sb.toString().replaceAll( invalidExp, METADATA_DIVIDER );
		}
	}
	
	/**
	 * 
	 * @param taskPath
	 * @param metadataKey
	 * @return
	 */
	public static String getMetadataItemFromTask( String taskPath, String metadataKey ) {
		String res = null;
		if ( taskPath != null && metadataKey != null && taskPath.contains( metadataKey ) ) {
			String[] taskPathSplit = taskPath.split( METADATA_DIVIDER );
			for ( String split : taskPathSplit ) {
				if ( split.contains( metadataKey ) ) {
					res = split.split( METADATA_KEY_VAL_DIVIDER )[1];
					break;
				}
			}
		}
		return res;
	}
	
	@Override
	public String toString() {
		return "Task:(path:{" + this.path + "} data:{" + this.data + "}" + " metadata:{" + this.metadata + "})";
	}
	

}
