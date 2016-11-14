/*******************************************************************************
 * Copyright (c) 2009, 2016 Erik Berglund.
 *    
 *        This file is part of Conserve.
 *    
 *        Conserve is free software: you can redistribute it and/or modify
 *        it under the terms of the GNU Affero General Public License as published by
 *        the Free Software Foundation, either version 3 of the License, or
 *        (at your option) any later version.
 *    
 *        Conserve is distributed in the hope that it will be useful,
 *        but WITHOUT ANY WARRANTY; without even the implied warranty of
 *        MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *        GNU Affero General Public License for more details.
 *    
 *        You should have received a copy of the GNU Affero General Public License
 *        along with Conserve.  If not, see <https://www.gnu.org/licenses/agpl.html>.
 *******************************************************************************/
package org.conserve.tools.metadata;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.conserve.adapter.AdapterBase;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.tools.Defaults;
import org.conserve.tools.ObjectTools;
import org.conserve.tools.Tools;
import org.conserve.tools.generators.NameGenerator;

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
		StringBuilder stmt = new StringBuilder("SELECT COLUMN_NAME,COLUMN_CLASS,COLUMN_SIZE FROM ");
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
			Long columnSize = rs.getLong(3);
			if(rs.wasNull())
			{
				columnSize = null;
			}
			Class<?> c = ObjectTools.lookUpClass(classDesc,adapter);
			this.returnTypes.add(c);
			this.props.add(name);
			this.setColumnSize(name,columnSize);
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

	@Override
	public String getColumnType(String prop)
	{
		String res = adapter.getColumnType(this.getReturnType(prop), null);
		Long size = getColumnSize(prop);
		if(size != null && res.contains("("))
		{
			//overwrite the size we got from the default column type
			String start = res.substring(0, res.indexOf("(")+1);
			String end = res.substring(res.indexOf(")"));
			res = start+Long.toString(size)+end;
		}
		
		return res;
	}

}
