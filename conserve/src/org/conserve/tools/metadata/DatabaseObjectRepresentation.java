package org.conserve.tools.metadata;

import org.conserve.adapter.AdapterBase;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.tools.NameGenerator;

/**
 * Metadata about an object loaded from the database.
 * 
 * @author Erik Berglund
 *
 */
public class DatabaseObjectRepresentation extends ObjectRepresentation
{
	/**
	 * Load the representation of the given class from the database.
	 * @param adapter 
	 * @param c the class to load.
	 * @param cw
	 */
	public DatabaseObjectRepresentation(AdapterBase adapter, Class<?> c, ConnectionWrapper cw)
	{
		this.clazz = c;
		this.adapter = adapter;
		this.tableName = NameGenerator.getTableName(c, adapter);
		doLoad(cw);
	}
	
	/**
	 * Load the class description from the database.
	 * @param cw
	 */
	private void doLoad(ConnectionWrapper cw)
	{
		// TODO Auto-generated method stub
		
	}
	
}
