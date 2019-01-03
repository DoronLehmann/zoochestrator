package com.imperva.zoochestrator.latch;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

/**
 * 
 * @author Doron Lehmann
 *
 */
public class WorkerNodeData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7813320021895482719L;

	/**
	 * The worker id
	 */
	public String id;

	/**
	 * The place to add any extra data for the worker
	 */
	public Map<String, Object> extraData;

	public WorkerNodeData( String id ) {
		this.id = id;
		this.extraData = new HashMap<String, Object>();
	}

	public WorkerNodeData( String id, Map<String, Object> extraData ) {
		this.id = id;
		if ( extraData == null ) {
			extraData = new HashMap<String, Object>();
		}
		this.extraData = extraData;
	}

	/**
	 * handler for serialize and deserialize the worker node data to/from a json element
	 */
	public static class WorkerNodeDataGsonHandler implements JsonDeserializer<WorkerNodeData>, JsonSerializer<WorkerNodeData> {

		private static final Logger logger = LoggerFactory.getLogger( WorkerNodeDataGsonHandler.class );
		public static final Gson gson = new Gson();

		@Override
		public JsonElement serialize( WorkerNodeData arg0, Type arg1, JsonSerializationContext arg2 ) {
			JsonObject res = new JsonObject();
			res.addProperty( "id", arg0.id );
			res.addProperty( "extraData", gson.toJson( arg0.extraData ) );
			return res;
		}

		@Override
		public WorkerNodeData deserialize( JsonElement elem, Type type, JsonDeserializationContext ctx ) throws JsonParseException {
			try {
				JsonElement extraData = elem.getAsJsonObject().get( "extraData" );
				String id = elem.getAsJsonObject().get( "id" ).getAsString();
				WorkerNodeData workerNodeData = new WorkerNodeData( id );
				Type t = new TypeToken<HashMap<String, String>>() {}.getType();
				workerNodeData.extraData = gson.fromJson( extraData, t );
				return workerNodeData;
			}
			catch ( Exception e ) {
				logger.error( "Cannot convert {} to workerConfigNodeData", elem.toString(), e );
				return null;
			}

		}
	}
}
