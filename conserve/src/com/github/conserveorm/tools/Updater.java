/*******************************************************************************
 *  
 * Copyright (c) 2009, 2017 Erik Berglund.
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

import java.lang.reflect.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.github.conserveorm.Persist;
import com.github.conserveorm.adapter.AdapterBase;
import com.github.conserveorm.connection.ConnectionWrapper;
import com.github.conserveorm.tools.generators.NameGenerator;
import com.github.conserveorm.tools.metadata.ObjectRepresentation;
import com.github.conserveorm.tools.metadata.ObjectStack;
import com.github.conserveorm.tools.protection.ProtectionManager;

/**
 * Class responsible for updating objects.
 * 
 * @author Erik Berglund
 * 
 */
public class Updater
{

	private AdapterBase adapter;

	public Updater(AdapterBase adapter)
	{
		this.adapter = adapter;
	}

	/**
	 * Update the object with the given database ID with the new values. Values
	 * that are not specified in nuValues will be left unchanged.
	 * 
	 * @param cw
	 * @param nuValues
	 *            the object that holds the new values to be assigned to the
	 *            database row.
	 * @param tableName
	 * @param databaseId
	 * 
	 * @throws SQLException
	 */
	public void updateObject(ConnectionWrapper cw, Object nuValues,
			String tableName, Long databaseId,
			DelayedInsertionBuffer delayBuffer) throws SQLException
	{
		if (nuValues.getClass().isArray())
		{
			updateArray(cw, nuValues, databaseId, delayBuffer);
		}
		else
		{
			ObjectStack oStack = new ObjectStack(adapter, nuValues.getClass(),
					nuValues);
			List<ObjectRepresentation>reps = oStack.getAllRepresentations();
			for(ObjectRepresentation rep:reps)
			{
				rep.setId(databaseId);
				Integer tableNameId = adapter.getPersist().getTableNameNumberMap().getNumber(cw, rep.getTableName());
				// get all existing reference values
				HashMap<String, Long> refValues = getReferenceValues(cw, rep);
				ArrayList<Object> values = new ArrayList<Object>();
				StringBuilder updateStatement = new StringBuilder();
				List<String> deleteList = new ArrayList<String>();
				for (Integer index : rep)
				{
					if (updateStatement.length() > 0)
					{
						updateStatement.append(",");
					}
					String name = rep.getPropertyName(index);
					Object value = rep.getPropertyValue(index);
					Class<?> referenceType = rep.getReturnType(index);
					updateStatement.append(name);
					updateStatement.append(" = ? ");
					if(referenceType.isEnum())
					{
						values.add(((Enum<?>)value).name());
					}
					else if(referenceType.equals(Class.class))
					{
						values.add(((Class<?>)value).getName());
					}
					else if (ObjectTools.isDatabasePrimitive(referenceType))
					{
						// put the new value in the statement
						values.add(value);
					}
					else
					{
						// save the new value
						Long propertyId = adapter.getPersist()
								.saveObjectUnprotected(cw, value, delayBuffer);
						if (propertyId == null)
						{
							break;
						}
						if (!propertyId.equals(refValues.get(name)))
						{
							// the new value is different from the existing
							// value, delete
							deleteList.add(name);
						}
						// protect the object
						adapter.getPersist()
								.getProtectionManager()
								.protectObjectInternalConditional(
										tableNameId,
										rep.getId(),
										rep.getPropertyName(index),
										adapter.getPersist().getTableNameNumberMap().getNumber(cw,value.getClass()),
										propertyId,
										adapter.getPersist().getClassNameNumberMap().getNumber(cw, value.getClass()), cw);
						// save the reference
						values.add(propertyId);
					}
				}
				// add all null values to the deleteList
				for (int y = 0; y < rep.getPropertyCount(); y++)
				{
					if (rep.getPropertyValue(y) == null)
					{
						deleteList.add(rep.getPropertyName(y));
					}
				}
				// delete complex values, if they exists
				if (deleteList.size() > 0)
				{
					nullProperties(cw, deleteList, refValues, oStack, rep);
				}
				if (updateStatement.length() > 0)
				{
					PreparedStatement pStatement = null;
					StringBuilder fullStatement = new StringBuilder("UPDATE ");
					fullStatement.append(rep.getTableName());
					fullStatement.append(" SET ");
					fullStatement.append(updateStatement);
					fullStatement.append(" WHERE ");

					fullStatement.append(Defaults.ID_COL);
					fullStatement.append(" = ?");
					pStatement = cw.prepareStatement(fullStatement.toString());
					for (int t = 0; t < values.size(); t++)
					{
						Object value = values.get(t);
						Tools.setParameter(pStatement, value.getClass(), t + 1,
								value,adapter);
					}
					pStatement.setLong(values.size() + 1, rep.getId());
					Tools.logFine(pStatement);

					int updatedCount = pStatement.executeUpdate();
					pStatement.close();
					if (updatedCount != 1)
					{
						throw new SQLException("Wrong number of rows updated: "
								+ updatedCount);
					}
				}
			}
		}
	}
 
	/**
	 * Get all reference (non-primitive) values for a given table entry.
	 * 
	 * @param rep
	 * @return
	 * @throws SQLException
	 */
	private HashMap<String, Long> getReferenceValues(ConnectionWrapper cw,
			ObjectRepresentation rep) throws SQLException
	{
		HashMap<String, Long> res = new HashMap<String, Long>();
		StringBuilder query = new StringBuilder("SELECT ");
		// get reference values
		ArrayList<String> refNames = new ArrayList<String>();
		for (int x = 0; x < rep.getPropertyCount(); x++)
		{
			if (!ObjectTools.isDatabasePrimitive(rep.getReturnType(x)))
			{
				refNames.add(rep.getPropertyName(x));
			}
		}
		if (refNames.size() > 0)
		{
			// add the names of the reference values
			for (int x = 0; x < refNames.size(); x++)
			{
				query.append(refNames.get(x));
				if (x < (refNames.size() - 1))
				{
					query.append(",");
				}
			}
			query.append(" FROM ");
			query.append(rep.getTableName());
			query.append(" WHERE ");
			query.append(Defaults.ID_COL);
			query.append(" = ?");
			PreparedStatement ps = cw.prepareStatement(query.toString());
			ps.setLong(1, rep.getId());
			Tools.logFine(ps);
			try
			{
				ResultSet rs = ps.executeQuery();
				if (rs.next())
				{
					// add name-value pairs to res
					ResultSetMetaData rsmd = rs.getMetaData();
					for (int x = 1; x <= rsmd.getColumnCount(); x++)
					{
						if (rs.getObject(x) != null)
						{
							res.put(rsmd.getColumnName(x).toUpperCase(),
									rs.getLong(x));
						}
					}
					if (rs.next())
					{
						throw new SQLException("Found more than one row in "
								+ rep.getTableName() + " with id "
								+ rep.getId());
					}
				}
				else
				{
					throw new SQLException("Found no row in "
							+ rep.getTableName() + " with id " + rep.getId());
				}
			}
			finally
			{
				ps.close();
			}
		}
		return res;
	}

	/**
	 * Update an array with a given ID.
	 * 
	 * @param cw
	 * @param nuValues
	 *            an array of the new values to store under the given id.
	 * @param databaseId
	 *            the id of the array to update.
	 * @throws SQLException
	 */
	private void updateArray(ConnectionWrapper cw, Object nuValues,
			Long databaseId, DelayedInsertionBuffer delayBuffer)
			throws SQLException
	{
		// if the new objects are non-primitve, save them and keep their ids
		ArrayList<TableId> nuIds = new ArrayList<TableId>();
		boolean isReferenceType = !ObjectTools.isDatabasePrimitive(nuValues.getClass().getComponentType());
		if (isReferenceType)
		{
			int length = Array.getLength(nuValues);
			for (int x = 0; x < length; x++)
			{
				Object nuObject = Array.get(nuValues, x);
				Long id = adapter.getPersist().saveObjectUnprotected(cw,
						nuObject, delayBuffer);
				if (id == null)
				{
					break;
				}
				nuIds.add(new TableId(NameGenerator.getTableName(nuObject,
						adapter), id));
			}
		}
		// Get type of existing array
		ArrayLoader arrayLoader = new ArrayLoader(adapter, adapter.getPersist()
				.getCache(), cw);
		arrayLoader.loadArray(databaseId);
		Class<?> componentType = arrayLoader.getArray().getClass()
				.getComponentType();
		//translate table names to corresponding IDs.
		// List all members of the existing array
		ProtectionManager protecter = adapter.getPersist()
				.getProtectionManager();
		if(isReferenceType)
		{
			for (int x = 0; x < arrayLoader.getLength(); x++)
			{
				Object component = arrayLoader.getEntry(x);
				// unprotect them
				Long ownerId = arrayLoader.getRelationalIds().get(x);
				protecter.unprotectObjectInternal(databaseId, ownerId, cw);

				// if they are non-primitive
				if (!ObjectTools.isDatabasePrimitive(component.getClass()))
				{
					String propertyTable = NameGenerator.getTableName(component,
							adapter);
					Long propertyId = adapter.getPersist().getId(component);
					protecter.unprotectObjectInternal(ownerId, propertyId, cw);
					if (!protecter.isProtected(propertyId, cw) && !nuIds
							.contains(new TableId(propertyTable, propertyId)))
					{
						// Delete them if they have no other protection
						adapter.getPersist().deleteObject(cw,
								component.getClass(), propertyId);
					}
				}
			}
		}
		// delete the old array_member entries
		StringBuilder removeMemberEntries = new StringBuilder("DELETE FROM ");
		removeMemberEntries.append(NameGenerator.getArrayMemberTableName(
				componentType, adapter));
		removeMemberEntries.append(" WHERE ");
		removeMemberEntries.append(Defaults.ARRAY_MEMBER_ID);
		removeMemberEntries.append(" = ? ");
		PreparedStatement removeStmt = cw.prepareStatement(removeMemberEntries
				.toString());
		removeStmt.setLong(1, databaseId);
		Tools.logFine(removeStmt);
		removeStmt.execute();
		removeStmt.close();

		adapter.getPersist().getArrayEntryWriter()
				.addArrayEntries(cw, databaseId, nuValues, delayBuffer);
	}

	/**
	 * Delete named entries from a named table and a row identified by
	 * databaseId.
	 * 
	 * @param cw
	 *            the connection wrapper to use.
	 * @param nullValues
	 *            the list of named values to delete.
	 * @throws SQLException
	 */
	private void nullProperties(ConnectionWrapper cw, List<String> nullValues,
			HashMap<String, Long> refValues, ObjectStack oStack,
			ObjectRepresentation rep) throws SQLException
	{
		ObjectRepresentation actual = oStack.getActualRepresentation();
		rep.setId( actual.getId());

		// set all the values to NULL
		PreparedStatement ps = null;
		StringBuilder statement = new StringBuilder("UPDATE ");
		statement.append(rep.getTableName());
		statement.append(" SET ");
		StringBuilder subStatement = new StringBuilder();
		for (String name : nullValues)
		{
			if (subStatement.length() > 0)
			{
				subStatement.append(",");
			}
			subStatement.append(name);
			subStatement.append(" = NULL");
		}
		statement.append(subStatement);
		statement.append(" WHERE ");

		statement.append(Defaults.ID_COL);
		statement.append(" = ?");
		ps = cw.prepareStatement(statement.toString());
		ps.setLong(1, rep.getId());
		Tools.logFine(ps);
		int updatedCount = ps.executeUpdate();
		ps.close();
		if (updatedCount != 1)
		{
			throw new SQLException("Wrong number of rows updated: "
					+ updatedCount);
		}
		// unprotect and optionally delete discarded values of the object
		Persist persist = adapter.getPersist();
		for (String nValue : nullValues)
		{
			Long propertyId = refValues.get(nValue);
			if (propertyId != null)
			{
				Class<?>returnType = oStack.getRepresentation(nValue).getReturnType(nValue);
				
				persist.getProtectionManager()
						.unprotectObjectInternal(rep.getId(),propertyId, cw);
				if (!persist.getProtectionManager()
						.isProtected(propertyId, cw))
				{
					persist.deleteObject(cw,returnType,propertyId);
				}
				
			}
		}
	}

	/**
	 * A pair, one table name and one table id.
	 * 
	 * @author Erik Berglund
	 */
	private static class TableId
	{
		private String tableName;
		private Long id;

		public TableId(String tableName, Long id)
		{
			this.id = id;
			this.tableName = tableName;
		}

		/**
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj)
		{
			if (obj instanceof TableId)
			{
				TableId other = (TableId) obj;
				if (tableName.equals(other.tableName) && id.equals(other.id))
				{
					return true;
				}
			}
			return false;
		}

	}
}
