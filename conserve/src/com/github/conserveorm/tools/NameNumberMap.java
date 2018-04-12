/*******************************************************************************
 *  
 * Copyright (c) 2009, 2018 Erik Berglund.
 *    
 *       This file is part of Conserve.
 *   
 *       Conserve is free software: you can redistribute it and/or modify
 *       it under the terms of the GNU Affero General Public License as published by
 *       the Free Software Foundation, either version 3 of the License, or
 *       (at your option) any later version.
 *   
 *       Conserve is distributed in the hope that it will be useful,
 *       but WITHOUT ANY WARRANTY; without even the implied warranty of
 *       MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *       GNU Affero General Public License for more details.
 *   
 *       You should have received a copy of the GNU Affero General Public License
 *       along with Conserve.  If not, see <https://www.gnu.org/licenses/agpl.html>.
 *       
 *******************************************************************************/
package com.github.conserveorm.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.github.conserveorm.adapter.AdapterBase;
import com.github.conserveorm.connection.ConnectionWrapper;

/**
 * Maintains a list of names and numbers. Each name has a number and vice versa.
 * Backed by a database table.
 * 
 * @author Erik Berglund
 *
 */
public class NameNumberMap
{
	protected AdapterBase adapter;
	private Map<Integer, String> numberToName = new HashMap<>();
	private Map<String, Integer> nameToNumber = new HashMap<>();
	private String tableName;

	public NameNumberMap(AdapterBase adapter, String tableName)
	{
		this.adapter = adapter;
		this.tableName = tableName;
	}

	/**
	 * Read the data from the database.
	 * 
	 * @throws SQLException
	 */
	public void loadData(ConnectionWrapper cw) throws SQLException
	{
		numberToName.clear();
		nameToNumber.clear();
		String query = "SELECT NAME," + Defaults.ID_COL + " FROM " + tableName;
		PreparedStatement stmt = cw.prepareStatement(query);
		Tools.logFine(stmt);
		ResultSet rs = stmt.executeQuery();
		while (rs.next())
		{
			String name = rs.getString(1);
			Integer number = rs.getInt(2);
			numberToName.put(number, name);
			nameToNumber.put(name, number);
		}
		stmt.close();
	}

	public void initialise(ConnectionWrapper cw) throws SQLException
	{
		if (!adapter.getPersist().getTableManager().tableExists(tableName, cw))
		{
			createTable(cw);
		}
		else
		{
			loadData(cw);
		}

	}

	/**
	 * Create and initialise the table backing this map.
	 * 
	 * @throws SQLException
	 */
	private void createTable(ConnectionWrapper cw) throws SQLException
	{
		String command = "CREATE TABLE " + tableName + " (" + Defaults.ID_COL + " ";
		if (adapter.isSupportsIdentity())
		{
			command += adapter.getIdentity();
		}
		else
		{
			command += adapter.getLongTypeKeyword();
		}
		command += " PRIMARY KEY, NAME " + adapter.getVarCharKeyword() + ")";
		PreparedStatement prepareStatement = cw.prepareStatement(command);
		Tools.logFine(prepareStatement);
		prepareStatement.execute();
		prepareStatement.close();
		if (!adapter.isSupportsIdentity())
		{
			adapter.getPersist().getTableManager().createTriggeredSequence(cw, tableName);
		}

		if (adapter.isRequiresCommitAfterSchemaAlteration())
		{
			cw.commit();
		}
	}

	/**
	 * Put a new name in the database. Afterwards the data is reloaded.
	 * 
	 * @param cw
	 * @param name
	 * @throws SQLException
	 */
	private void saveName(ConnectionWrapper cw, String name) throws SQLException
	{
		String command = "INSERT INTO " + tableName + " (NAME) VALUES (?)";
		PreparedStatement prepareStatement = cw.prepareStatement(command);
		prepareStatement.setString(1, name);
		Tools.logFine(prepareStatement);
		prepareStatement.execute();
		prepareStatement.close();
		loadData(cw);
	}

	/**
	 * Get the name matching a given number.
	 * 
	 * @param cw
	 *            the connection wrapper to use to update the map in case the
	 *            number is unknown.
	 * @param number
	 * @throws SQLException
	 */
	public String getName(ConnectionWrapper cw, Integer number) throws SQLException
	{
		if(number == null)
		{
			return null;
		}
		String res = numberToName.get(number);
		if (res == null)
		{
			// a new name has popped up in the database, load it
			loadData(cw);
			res = numberToName.get(number);
		}
		return res;
	}

	public Integer getNumber(ConnectionWrapper cw, String name) throws SQLException
	{
		if(name == null)
		{
			return null;
		}
		Integer res = nameToNumber.get(name);
		if (res == null)
		{
			// has an unknown name appeared in the database?
			loadData(cw);
			res = nameToNumber.get(name);
			if (res == null)
			{
				// nope, it must be one we have created
				saveName(cw, name);
				res = nameToNumber.get(name);
			}
		}
		return res;
	}

}
