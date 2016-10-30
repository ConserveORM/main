package org.conserve.tools;

import java.sql.SQLException;

import org.conserve.adapter.AdapterBase;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.tools.generators.NameGenerator;

/**
 * NameNumberMap subclass that keeps track of table names.
 * 
 * @author Erik Berglund
 *
 */
public class TableNameNumberMap extends NameNumberMap
{

	/**
	 * @param adapter
	 * @param tableName
	 */
	public TableNameNumberMap(AdapterBase adapter)
	{
		super(adapter, Defaults.TABLE_NAME_MAP_TABLE);
	}
	
	/**
	 * Get the number for the table name associated with the given class. Insert the table name if it does not exist.
	 * 
	 * @param cw
	 * @param clazz
	 * @return
	 * @throws SQLException 
	 */
	public Integer getNumber(ConnectionWrapper cw, Class<?>clazz) throws SQLException
	{
		return getNumber(cw,NameGenerator.getTableName(clazz, adapter));
	}

}
