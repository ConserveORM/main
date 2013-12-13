/*******************************************************************************
 * Copyright (c) 2009, 2013 Erik Berglund.
 *   
 *      This file is part of Conserve.
 *   
 *       Conserve is free software: you can redistribute it and/or modify
 *       it under the terms of the GNU Lesser General Public License as published by
 *       the Free Software Foundation, either version 3 of the License, or
 *       (at your option) any later version.
 *   
 *       Conserve is distributed in the hope that it will be useful,
 *       but WITHOUT ANY WARRANTY; without even the implied warranty of
 *       MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *       GNU Lesser General Public License for more details.
 *   
 *       You should have received a copy of the GNU Lesser General Public License
 *       along with Conserve.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.conserve.tools.generators;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.Map.Entry;

import org.conserve.adapter.AdapterBase;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.tools.Defaults;
import org.conserve.tools.TableManager;
import org.conserve.tools.Tools;
import org.conserve.tools.metadata.ObjectRepresentation;
import org.conserve.tools.metadata.ObjectStack;
import org.conserve.tools.uniqueid.UniqueIdGenerator;
import org.conserve.tools.uniqueid.UniqueIdTree;

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
	 * @param toStack
	 * @param subClass
	 * @param cw
	 * 
	 * @throws SQLException
	 */
	public void move(ObjectStack fromStack, ObjectStack toStack, ObjectRepresentation subClass, ConnectionWrapper cw)
			throws SQLException
	{
		// make sure the new table has all the right columns
		for (int prop = 0; prop < subClass.getPropertyCount(); prop++)
		{
			String colName = subClass.getPropertyName(prop);
			Class<?> propClass = subClass.getReturnType(prop);
			adapter.getPersist().getTableManager().ensureColumnExists(subClass.getTableName(), colName, propClass, cw);
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
		// get all objects of the from class table
		StringBuilder statement = new StringBuilder("SELECT ");
		statement.append(Defaults.ID_COL);
		statement.append(" FROM ");
		statement.append(subClass.getTableName());
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		while (rs.next())
		{
			int currentLevel = fromStack.getSize() - 1;
			// create a new entry in the to-tables
			ObjectStack nuStack = new ObjectStack(adapter, subClass.getRepresentedClass());
			nuStack.saveNoCache(cw, lowestCommonSubClassLevel + 1);
			// get the to-id of the highest non-common subclass
			long nuId = nuStack.getRepresentation(lowestCommonSubClassLevel + 1).getId();
			// cast the id to the correct level
			Long fromId = rs.getLong(Defaults.ID_COL);
			long oldId = getCastIdDatabase(fromStack, lowestCommonSubClassLevel + 1, subClass, fromId, cw);
			// rewrite the pointers C__ID and C__REALCLASS in the lowest common
			// subclass
			updateSubClassRef(nuStack, fromStack, oldId, nuStack, nuId, lowestCommonSubClassLevel, cw);
			// get the id at the current level
			// iterate over the fromtables
			while (currentLevel > lowestCommonSubClassLevel)
			{
				// pick up properties, move to target
				String fromTableName = fromStack.getRepresentation(currentLevel).getTableName();
				String fromClassName = fromStack.getRepresentation(currentLevel).getSystemicName();
				moveFilelds(fromTableName, fromId, nuStack, cw);

				// drop row from fromtable
				dropRow(fromTableName, fromId, cw);
				// go up a level
				currentLevel--;
				// get the next id
				fromId = getParentId(fromStack.getRepresentation(currentLevel).getTableName(), fromClassName, fromId,
						cw);

			}
			// TODO: What to do with protection entries?
		}
		// remove columns from subclasstable where they are no longer in the
		// class
		try
		{
			TableManager tm = adapter.getPersist().getTableManager();
			ObjectRepresentation objRes = toStack.getActualRepresentation();
			Map<String, String> valueTypeMap = tm.getDatabaseColumns(objRes.getTableName(),
					cw);
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
	 * Get the id of the object represented by id fromId at level subClass, as
	 * it appears at level.
	 * 
	 * @param fromStack
	 * @param level
	 *            the output level.
	 * @param subClass
	 * @param fromId
	 * @param cw
	 * @return
	 * @throws SQLException
	 */
	private Long getCastIdDatabase(ObjectStack fromStack, int level, ObjectRepresentation subClass, Long fromId,
			ConnectionWrapper cw) throws SQLException
	{
		int targetLevel = fromStack.getLevel(subClass.getRepresentedClass());
		if (level == targetLevel)
		{
			// no need to change it, as it's already cast to the correct class.
			return fromId;
		}
		else
		{
			UniqueIdGenerator generator = new UniqueIdGenerator();
			UniqueIdTree tree = new UniqueIdTree(generator);
			tree.nameStack(fromStack);
			IdStatementGenerator idGen = new IdStatementGenerator(adapter, fromStack, true);
			String idStatement = idGen.generate(targetLevel);
			StringBuilder sb = new StringBuilder("SELECT ");
			sb.append(fromStack.getRepresentation(level).getAsName());
			sb.append(".");
			sb.append(Defaults.ID_COL);
			sb.append(" FROM ");
			sb.append(idGen.generateAsStatement());
			sb.append(" WHERE ");
			sb.append(idStatement);
			PreparedStatement ps = cw.prepareStatement(sb.toString());

			int index = 0;
			for (RelationDescriptor o : idGen.getRelationDescriptors())
			{
				if (o.isRequiresvalue())
				{
					index++;
					Object val = o.getValue();

					Tools.setParameter(ps, val.getClass(), index, val);
				}
			}
			Tools.logFine(ps);
			try
			{
				ResultSet rs = ps.executeQuery();
				if (rs.next())
				{
					return rs.getLong(1);
				}
				else
				{
					return null;
				}
			}
			finally
			{
				ps.close();
			}
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
	private void moveFilelds(String fromTableName, long fromId, ObjectStack nuStack, ConnectionWrapper cw)
			throws SQLException
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
						copyProperty(fromTableName, rs, x, mdata.getColumnType(x), colName, toTable,
								targetLevel.getId(), cw);
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
	private void copyProperty(String fromTableName, ResultSet rs, int x, int columnType, String colName,
			String toTable, long toId, ConnectionWrapper cw) throws SQLException
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
		if (colName.equals(Defaults.ID_COL) || colName.equals(Defaults.REAL_ID_COL)
				|| colName.equals(Defaults.REAL_CLASS_COL))
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
	 * Get the C__ID field of tableMame that has real class = fromClassName and
	 * real id = fromId
	 * 
	 * @param tableName
	 * @param fromClassName
	 * @param fromId
	 * @return
	 * @throws SQLException
	 */
	private Long getParentId(String tableName, String fromClassName, long fromId, ConnectionWrapper cw)
			throws SQLException
	{
		Long res = null;
		StringBuilder sb = new StringBuilder("SELECT ");
		sb.append(Defaults.ID_COL);
		sb.append(" FROM ");
		sb.append(tableName);
		sb.append(" WHERE ");
		sb.append(Defaults.REAL_ID_COL);
		sb.append("=? AND ");
		sb.append(Defaults.REAL_CLASS_COL);
		sb.append("=?");
		PreparedStatement ps = cw.prepareStatement(sb.toString());
		ps.setLong(1, fromId);
		ps.setString(2, fromClassName);
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		if (rs.next())
		{
			res = rs.getLong(Defaults.ID_COL);
		}
		ps.close();
		return res;
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
	private void updateSubClassRef(ObjectStack nuStack, ObjectStack fromClass, long oldId, ObjectStack toClass,
			long nuId, int lowestCommonSubclass, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder sb = new StringBuilder("UPDATE ");
		String tableName = nuStack.getRepresentation(lowestCommonSubclass).getTableName();
		sb.append(tableName);
		sb.append(" SET ");
		sb.append(Defaults.REAL_ID_COL);
		sb.append("=?, ");
		sb.append(Defaults.REAL_CLASS_COL);
		sb.append("=? WHERE ");
		sb.append(Defaults.REAL_ID_COL);
		sb.append("=? AND ");
		sb.append(Defaults.REAL_CLASS_COL);
		sb.append("=?");
		PreparedStatement ps = cw.prepareStatement(sb.toString());
		ps.setLong(1, nuId);
		ps.setString(2, toClass.getRepresentation(lowestCommonSubclass + 1).getSystemicName());
		ps.setLong(3, oldId);
		ps.setString(4, fromClass.getRepresentation(lowestCommonSubclass + 1).getSystemicName());
		Tools.logFine(ps);
		ps.executeUpdate();
		ps.close();
	}
}
