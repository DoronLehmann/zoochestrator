package com.imperva.zoochestrator.utils;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import com.google.common.base.Joiner;
import com.imperva.zoochestrator.tasks.Task;

/**
 * Utils class for common zookeeper use cases
 * 
 */
public class ZookeeperUtils {

	public static final String TASK_REASSIGNMENT_CONFIG_PROPERTY = "zookeeper.task.reassignment.enabled";

	private static final String REGEX_VALID_NODE_PATH = "(\\/[^\\/\\s]+)+";

	/**
	 * delete and then create znode for the given path. The action is taken within transaction so either both actions occurred or non of them.<br>
	 * Note: won't work if the note have children or if it doesn't exist.<br>
	 * Useful for EPHEMERAL znodes when we want to make sure the current ZK session is the owner of the ephemeral znode
	 * @param client - CuratorFramework client 
	 * @param path - path of the node
	 * @param mode - the creation mode for the node ( PERSISTENT, PERSISTENT_SEQUENTIAL, EPHEMERAL or EPHEMERAL_SEQUENTIAL)
	 * @throws Exception on error
	 */
	public static void recreateNode( CuratorFramework client, String path, CreateMode mode, String data) throws Exception {
		client.inTransaction().
		delete().forPath(path).
		and().
		create().withMode( mode ).forPath(path, toBytes(data)).
		and().
		commit();
	}

	public static Task createTask( List<Long> batch, String path ) {
		String csvItems = Joiner.on( "," ).join( batch );
		return new Task( csvItems, path );
	}

    /**
     * Takes a variable number of strings as arguments representing different nodes in a zookeeper path,<br>
     * and returns the full path from those nodes based on the order they were given.<br>
     * For example: the input <code>(scope, tasks, taskName)</code> will return <code>"/scope/tasks/taskName"</code>.
     * @param nodes The nodes to generate the full path from
     * @return The full path starting with <code>"/"</code>
     *         or null if any of the nodes are null or empty or the method is called with no arguments.
     */
    public static String generatePath(String... nodes) {
        if (nodes == null || nodes.length == 0) {
            return null;
        }
        for (String node : nodes) {
            if (isEmpty(node)) {
                return null;
            }
        }
        // Scope is usually provided with root ('/') as the starting character - if not, add it as prefix
        String prefix = "";
        if (!(nodes[0].charAt(0) == '/')) {
            prefix = "/";
        }
        return prefix + Joiner.on("/").join(nodes);
    }

    /**
     * Extracts the scope (a.k.a base path) from the given node if the node is of a valid form.<br>
	 * For example: the input <code>"/scope/tasks/taskName"</code> will return <code>"/scope"</code>.
     * @param node The Zookeeper node to extract the scope from
     * @return The scope (base path) of the given node
     * @throws IllegalArgumentException If the given node has an unrecognized form
     */
    public static String extractScope(String node) throws IllegalArgumentException {
        if (isEmpty(node) || !node.matches(REGEX_VALID_NODE_PATH)) {
            final String errorMessage = "Failed to extract scope from node - illegal string passed as parameter";
            throw new IllegalArgumentException(errorMessage);
        }
        return "/" + node.split("/")[1];
    }
    
    /**
	 * Check if a string is empty (null or has length 0)
	 *
	 * @param str the input string
	 * @return true is the string is empty
	 */
	public static boolean isEmpty( String str ) {
		return str == null || str.trim().length() == 0 || "null".equals( str );
	}

	/**
	 * Creates a UTF-8 String from byte array. Use this method to ensure<br>
	 * that the byte array is decoded in UTF-8 to avoid ambiguity of encoding.
	 */
	public static String fromBytes(byte[] bytes) {
	    return new String(bytes, StandardCharsets.UTF_8);
	}
	
	/**
	 * Creates a UTF-8 encoded byte array from a String. Use this method to ensure<br>
	 * that the byte array is encoded in UTF-8 to avoid ambiguity of encoding.
	 */
	public static byte[] toBytes(String str) {
	    return str.getBytes(StandardCharsets.UTF_8);
	}
	
	/**
	 * Check if a string is not empty (not null and has length greater than 0)
	 *
	 * @param str the input string
	 * @return true is the string is not empty
	 */
	public static boolean isNotEmpty( String str ) {
		return !isEmpty( str );
	}
}
