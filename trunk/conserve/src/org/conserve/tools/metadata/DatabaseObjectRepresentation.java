package org.conserve.tools.metadata;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.conserve.adapter.AdapterBase;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.tools.Defaults;
import org.conserve.tools.NameGenerator;
import org.conserve.tools.ObjectTools;
import org.conserve.tools.Tools;

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
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public DatabaseObjectRepresentation(AdapterBase adapter, Class<?> c, ConnectionWrapper cw) throws SQLException, ClassNotFoundException
	{
		this.clazz = c;
		this.adapter = adapter;
		this.tableName = NameGenerator.getTableName(c, adapter);
		doLoad(cw);
	}
	
	/**
	 * Load the class description from the database.
	 * @param cw
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	private void doLoad(ConnectionWrapper cw) throws SQLException, ClassNotFoundException
	{
		//get the column names and types from the database
		StringBuilder stmt = new StringBuilder("SELECT COLUMN_NAME,COLUMN_CLASS FROM ");
		stmt.append(Defaults.TYPE_TABLENAME);
		stmt.append(" WHERE OWNER_TABLE = ?");
		PreparedStatement ps = cw.prepareStatement(stmt.toString());
		ps.setString(1, getTableName());
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		while(rs.next())
		{
			String name = rs.getString(1);
			String classDesc = rs.getString(2);
			Class<?> c = ObjectTools.lookUpClass(classDesc,adapter);
			this.returnTypes.add(c);
			this.props.add(name);
		}
		ps.close();
		
		//get the indices
		stmt = new StringBuilder("SELECT INDEX_NAME,COLUMN_NAME FROM ");
		stmt.append(Defaults.INDEX_TABLENAME);
		stmt.append(" WHERE TABLE_NAME = ?");
		ps = cw.prepareStatement(stmt.toString());
		ps.setString(1, tableName);
		Tools.logFine(ps);
		rs = ps.executeQuery();
		while(rs.next())
		{
			String indexName = rs.getString(1);
			String columName = rs.getString(2);
			List<String>idxList = indices.get(columName);
			if(idxList == null)
			{
				idxList = new ArrayList<String>();
			}
			if(!idxList.contains(indexName))
			{
				idxList.add(indexName);
			}
			indices.put(columName, idxList);
		}
		ps.close();
		buildIndexMap();
	}
	
}
