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
package org.conserve.tools.generators;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.conserve.adapter.AdapterBase;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.tools.Defaults;
import org.conserve.tools.TableManager;
import org.conserve.tools.Tools;
import org.conserve.tools.metadata.ObjectRepresentation;
import org.conserve.tools.metadata.ObjectStack;
import org.conserve.tools.protection.ProtectionManager;

/**
 * Generates statements that copies all values in one column from one table to
 * another.
 * 
 * @author Erik Berglund
 * 
 */
public class SubclassMover
{
	private AdapterBase adapter;

	public SubclassMover(AdapterBase adapter)
	{
		this.adapter = adapter;
	}

	/**
	 * Move subClass from fromStack to toStack.
	 * 
	 * @param fromStack
	 *            the inheritance stack the subclass is moved from
	 * @param toStack
	 *            the inheritance stack the subclass is moved to
	 * @param subClass
	 *            the class to move from fromStack to toStack
	 * @param cw
	 * 
	 * @throws SQLException
	 */
	public void move(ObjectStack fromStack, ObjectStack toStack, ObjectRepresentation subClass, ConnectionWrapper cw) throws SQLException
	{
		// make sure the new table has all the right columns
		for (int prop = 0; prop < subClass.getPropertyCount(); prop++)
		{
			String colName = subClass.getPropertyName(prop);
			Class<?> propClass = subClass.getReturnType(prop);
			adapter.getPersist().getTableManager().ensureColumnExists(subClass.getTableName(), colName, propClass,subClass.getColumnSize(colName), cw);
		}

		// find the lowest common superclass
		int min = Math.min(fromStack.getSize(), toStack.getSize());
		int lowestCommonSubClassLevel = 0;
		for (int level = 1; level < min; level++)
		{
			Class<?> c1 = fromStack.getRepresentation(level).getRepresentedClass();
			Class<?> c2 = toStack.getRepresentation(level).getRepresentedClass();
			if (c1.equals(c2))
			{
				lowestCommonSubClassLevel = level;
			}
			else
			{
				break;
			}
		}

		// get the ID of all objects of the from class table
		List<Long> idsToChange = new ArrayList<Long>();
		StringBuilder statement = new StringBuilder("SELECT ");
		statement.append(Defaults.ID_COL);
		statement.append(" FROM ");
		statement.append(subClass.getTableName());
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		while (rs.next())
		{
			Long fromId = rs.getLong(Defaults.ID_COL);
			idsToChange.add(fromId);
		}
		ps.close();

		// get a protection manager instance
		ProtectionManager pm = new ProtectionManager();
		// iterate over the C__ID values of all entries to change
		for (Long fromId : idsToChange)
		{
			int currentLevel = fromStack.getSize() - 1;
			// create a new entry in the to-tables
			ObjectStack nuStack = new ObjectStack(adapter, subClass.getRepresentedClass());
			nuStack.saveNoCache(cw, lowestCommonSubClassLevel + 1);

			// rewrite the pointers  C__REALCLASS in the lowest common
			// subclass
			updateSubClassRef(nuStack, fromStack, nuStack, lowestCommonSubClassLevel, cw);

			// get the id of the newly inserted object
			long toId = nuStack.getActualRepresentation().getId();
			if (fromId != toId)
			{
				// rewrite protection entries from lowestFromId to toId
				pm.changeObjectId(subClass.getTableName(), fromId, toId, cw);
			}

			// get the id at the current level
			// iterate over the from tables
			while (currentLevel > lowestCommonSubClassLevel)
			{
				// pick up properties, move to target
				String fromTableName = fromStack.getRepresentation(currentLevel).getTableName();
				moveFields(fromTableName, fromId, nuStack, cw);

				// drop row from fromtable
				dropRow(fromTableName, fromId, cw);
				// go up a level
				currentLevel--;
			}
		}
		// remove columns from subclass table where they are no longer in the
		// class
		try
		{
			TableManager tm = adapter.getPersist().getTableManager();
			ObjectRepresentation objRes = toStack.getActualRepresentation();
			Map<String, String> valueTypeMap = tm.getDatabaseColumns(objRes.getTableName(), cw);
			for (Entry<String, String> e : valueTypeMap.entrySet())
			{
				String colName = e.getKey();
				if (!isSystemColumn(colName) && !objRes.hasProperty(colName))
				{
					tm.dropColumn(objRes.getTableName(), colName, cw);
				}
			}
		}
		catch (ClassNotFoundException ex)
		{
			throw new SQLException(ex);
		}
	}


	/**
	 * Get all the fields in the table, move the fields to corresponding class
	 * in nuStack
	 * 
	 * @param fromTableName
	 * @param fromId
	 * @param nuStack
	 * @param cw
	 * @throws SQLException
	 */
	private void moveFields(String fromTableName, long fromId, ObjectStack nuStack, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder sb = new StringBuilder("SELECT * FROM ");
		sb.append(fromTableName);
		sb.append(" WHERE ");
		sb.append(Defaults.ID_COL);
		sb.append(" = ?");
		PreparedStatement ps = cw.prepareStatement(sb.toString());
		ps.setLong(1, fromId);
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		if (rs.next())
		{
			ResultSetMetaData mdata = rs.getMetaData();
			for (int x = 1; x <= mdata.getColumnCount(); x++)
			{
				String colName = mdata.getColumnLabel(x);
				if (!isSystemColumn(colName))
				{
					ObjectRepresentation targetLevel = nuStack.getRepresentation(colName);
					if (targetLevel != null)
					{
						String toTable = targetLevel.getTableName();
						copyProperty(fromTableName, rs, x, mdata.getColumnType(x), colName, toTable, targetLevel.getId(), cw);
					}
				}
			}
		}
	}

	/**
	 * @param fromTableName
	 * @param rs
	 * @param x
	 * @param columnType
	 * @param colName
	 * @param toTable
	 * @param cw
	 * @throws SQLException
	 */
	private void copyProperty(String fromTableName, ResultSet rs, int x, int columnType, String colName, String toTable, long toId,
			ConnectionWrapper cw) throws SQLException
	{

		if (!fromTableName.equals(toTable))
		{
			StringBuilder sb = new StringBuilder("UPDATE ");
			sb.append(toTable);
			sb.append(" SET ");
			sb.append(colName);
			sb.append("=? WHERE ");
			sb.append(Defaults.ID_COL);
			sb.append("=?");
			PreparedStatement ps = cw.prepareStatement(sb.toString());
			ps.setLong(2, toId);
			switch (columnType)
			{
				case Types.BIT:
					ps.setBoolean(1, rs.getBoolean(x));
					break;
				case Types.TINYINT:
					ps.setByte(1, rs.getByte(x));
					break;
				case Types.SMALLINT:
					ps.setShort(1, rs.getShort(x));
					break;
				case Types.INTEGER:
					ps.setInt(1, rs.getInt(x));
					break;
				case Types.BIGINT:
					ps.setLong(1, rs.getLong(x));
					break;
				case Types.FLOAT:
					ps.setFloat(1, rs.getFloat(x));
					break;
				case Types.DOUBLE:
					ps.setDouble(1, rs.getDouble(x));
					break;
				case Types.DATE:
					ps.setDate(1, rs.getDate(x));
					break;
				case Types.TIME:
					ps.setTime(1, rs.getTime(x));
					break;
				case Types.TIMESTAMP:
					ps.setTimestamp(1, rs.getTimestamp(x));
					break;
				case Types.LONGVARCHAR:
				case Types.VARCHAR:
				case Types.CHAR:
					ps.setString(1, rs.getString(x));
					break;
				case Types.BINARY:
				case Types.VARBINARY:
				case Types.LONGVARBINARY:
					ps.setBlob(1, rs.getBlob(x));
					break;
				default:
					throw new SQLException("Unknown column type " + columnType + " for column " + colName);
			}
			try
			{
				Tools.logFine(ps);
				ps.executeUpdate();
			}
			finally
			{
				ps.close();
			}
		}
	}

	/**
	 * @param colName
	 * @return
	 */
	private boolean isSystemColumn(String colName)
	{
		if (colName.equals(Defaults.ID_COL) || colName.equals(Defaults.REAL_CLASS_COL))
		{
			return true;
		}
		return false;
	}

	/**
	 * Drop a row with the given C__ID from table.
	 * 
	 * @param table
	 * @param id
	 * @throws SQLException
	 */
	private void dropRow(String table, long id, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder sb = new StringBuilder("DELETE FROM ");
		sb.append(table);
		sb.append(" WHERE ");
		sb.append(Defaults.ID_COL);
		sb.append("=?");
		PreparedStatement ps = cw.prepareStatement(sb.toString());
		ps.setLong(1, id);
		Tools.logFine(ps);
		ps.executeUpdate();
		ps.close();
	}

	/**
	 * @param nuStack
	 *            The stack of the owner class
	 * @param fromClass
	 *            the stack of the old subclass
	 * @param oldId
	 *            the id of the entry in the old subclass
	 * @param toClass
	 *            the stack of the new subclass
	 * @param nuId
	 *            the id of the entry in the new subclass
	 * @param lowestCommonSubclass
	 *            the level the update is happening at.
	 * @throws SQLException
	 */
	private void updateSubClassRef(ObjectStack nuStack, ObjectStack fromClass, ObjectStack toClass, int lowestCommonSubclass,
			ConnectionWrapper cw) throws SQLException
	{
		StringBuilder sb = new StringBuilder("UPDATE ");
		sb.append(nuStack.getRepresentation(lowestCommonSubclass).getTableName());
		sb.append(" SET ");
		sb.append(Defaults.REAL_CLASS_COL);
		sb.append("=? WHERE ");
		sb.append(Defaults.REAL_CLASS_COL);
		sb.append("=?");
		PreparedStatement ps = cw.prepareStatement(sb.toString());
		ps.setString(1, toClass.getRepresentation(lowestCommonSubclass + 1).getSystemicName());
		ps.setString(2, fromClass.getRepresentation(lowestCommonSubclass + 1).getSystemicName());
		Tools.logFine(ps);
		ps.executeUpdate();
		ps.close();
	}
}
