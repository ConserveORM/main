package org.conserve.tools;

import java.sql.SQLException;

import org.conserve.adapter.AdapterBase;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.tools.generators.NameGenerator;

/**
 * NameNumberMap subclass that keeps track of class names.
 * @author Erik Berglund
 *
 */
public class ClassNameNumberMap extends NameNumberMap
{

	/**
	 * @param adapter
	 */
	public ClassNameNumberMap(AdapterBase adapter)
	{
		super(adapter, Defaults.CLASS_NAME_MAP_TABLE);
	}
	
	/**
	 * Get the number associated with a given class.
	 * @param cw the ConnectionWrapper to use in case the number is not in the cache.
	 * @param clazz
	 * @throws SQLException 
	 */
	public Integer getNumber(ConnectionWrapper cw, Class<?>clazz) throws SQLException
	{
		return getNumber(cw,NameGenerator.getSystemicName(clazz));
	}
}
