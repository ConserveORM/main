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
package org.conserve.tools.protection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.conserve.Persist;
import org.conserve.adapter.AdapterBase;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.tools.Defaults;
import org.conserve.tools.Tools;

/**
 * Responsible for the reference-counting based protection scheme.
 * This class uses table and class name identifiers instead of actual table and class names.
 * To convert between table names and table name identifiers, class names and class name identifiers,
 * and vice verse, @see org.conserve.tools.TableNameNumberMap and @see org.conserve.tools.ClassNameNumberMap.
 * 
 * @author Erik Berglund
 * 
 */
public class ProtectionManager
{
	private AdapterBase adapter;

	public ProtectionManager(AdapterBase adapter)
	{
		this.adapter = adapter;
	}
	/**
	 * Check if an object, identified by tablename id and id, is protected
	 * This checks references recursively because an object can contain itself.
	 * 
	 * @param tableId
	 * @param databaseId
	 * @param cw
	 * @return
	 * @throws SQLException
	 */
	public boolean isProtected(Integer tableId, Long databaseId, ConnectionWrapper cw) throws SQLException
	{
		DependentSet depSet = new DependentSet(tableId, databaseId, cw);
		return depSet.isProtected();
	}
	

	/**
	 * Check if a given table id and db id combination is protected.
	 * 
	 * @param tableId the id of the table - @see org.conserve.tools.TableNameNumberMap 
	 * @param databaseId
	 * @param cw
	 * @return
	 * @throws SQLException
	 */
	public boolean isProtectedExternal(Integer tableId, Long databaseId, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder statement = new StringBuilder(150);
		statement.append("SELECT * FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE OWNER_TABLE IS NULL AND OWNER_ID IS NULL AND PROPERTY_TABLE = ? AND PROPERTY_ID = ?");
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		ps.setInt(1, tableId);
		ps.setLong(2, databaseId);
		Tools.logFine(ps);
		try
		{
			ResultSet rs = ps.executeQuery();
			if (rs.next())
			{
				return true;
			}
		}
		finally
		{
			ps.close();
		}
		return false;
	}

	/**
	 * Label the object as having outside reference.
	 * 
	 * @param tableId the id of the table - @see org.conserve.tools.TableNameNumberMap 
	 * @param databaseId
	 * @param classNameId the id of the protected class, @see org.conserve.tools.ClassNameNumberMap
	 * @param cw
	 * @throws SQLException
	 */
	public void protectObjectExternal(Integer tableId, Long databaseId, Integer classNameId, ConnectionWrapper cw)
			throws SQLException
	{
		// ensure that the object is labelled as coming from outside
		PreparedStatement ps = cw.prepareStatement("INSERT INTO " + Defaults.HAS_A_TABLENAME
				+ " (PROPERTY_TABLE,PROPERTY_ID,PROPERTY_CLASS) values (?,?,?)");
		try
		{
			ps.setInt(1, tableId);
			ps.setLong(2, databaseId);
			ps.setInt(3, classNameId);
			Tools.logFine(ps);
			ps.execute();
		}
		finally
		{
			ps.close();
		}
	}
	
	/**
	 * Convenience method that converts ownerTableName, propertyTableName, and propertyClassName to IDs before calling
	 * {@link #protectObjectInternal(Integer, Long, String, Integer, Long, Integer, ConnectionWrapper)}
	 * 
	 * @param ownerTableName
	 * @param ownerId
	 * @param relationName
	 * @param propertyTableName
	 * @param propertyId
	 * @param propertyClassName
	 * @param cw
	 * @throws SQLException 
	 */
	public void protectObjectInternal(String ownerTableName, Long ownerId, String relationName,
			String propertyTableName, Long propertyId, String propertyClassName, ConnectionWrapper cw) throws SQLException
	{
		Persist p = adapter.getPersist();
		Integer ownerTableNameId = p.getTableNameNumberMap().getNumber(cw, ownerTableName);
		Integer propertyTableNameId = p.getTableNameNumberMap().getNumber(cw, propertyTableName);
		Integer propertyClassNameId = p.getClassNameNumberMap().getNumber(cw, propertyClassName);
		protectObjectInternal(ownerTableNameId, ownerId, relationName, propertyTableNameId, propertyId, propertyClassNameId, cw);
	}

	/**
	 * Create a protection entry for the given owner-property relationship.
	 * 
	 * @param ownerTableNameId
	 *            the id of the table name of the owning object.
	 * @param ownerId
	 *            the database id of the owning object.
	 * @param relationName
	 *            the name the property has within the owner.
	 * @param propertyTableNameId
	 *            the table name id of the owned object.
	 * @param propertyId
	 *            the database id of the owned object.
	 * @param propertyClassId
	 *            the id of thecanonical name of the actual property class.
	 * @param cw
	 *            the connection to the database.
	 * @throws SQLException
	 */
	public void protectObjectInternal(Integer ownerTableNameId, Long ownerId, String relationName,
			Integer propertyTableNameId, Long propertyId, Integer propertyClassId, ConnectionWrapper cw) throws SQLException
	{
		// ensure that the object is labelled as coming from inside
		PreparedStatement ps = cw.prepareStatement("INSERT INTO " + Defaults.HAS_A_TABLENAME
				+ " (OWNER_TABLE,OWNER_ID,PROPERTY_TABLE,PROPERTY_ID,PROPERTY_CLASS," + Defaults.RELATION_NAME_COL
				+ ") values (?,?,?,?,?,?)");
		try
		{
			ps.setInt(1, ownerTableNameId);
			ps.setLong(2, ownerId);
			ps.setInt(3, propertyTableNameId);
			ps.setLong(4, propertyId);
			if (propertyClassId == null)
			{
				ps.setNull(5, java.sql.Types.INTEGER);
			}
			else
			{
				ps.setInt(5, propertyClassId);
			}
			if(relationName == null)
			{
				ps.setNull(6, java.sql.Types.INTEGER);
			}
			else
			{
				ps.setInt(6, adapter.getPersist().getColumnNameNumberMap().getNumber(cw, relationName));
			}
			Tools.logFine(ps);
			ps.execute();
		}
		finally
		{
			ps.close();
		}
	}

	/**
	 * Remove the outside-reference label from the given object.
	 * 
	 * @param tableNameId
	 * @param databaseId
	 * @param cw
	 *            the connection object.
	 * 
	 * @throws SQLException
	 */
	public void unprotectObjectExternal(Integer tableNameId, Long databaseId, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder statement = new StringBuilder(150);
		statement.append("DELETE FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE OWNER_TABLE IS NULL AND PROPERTY_TABLE = ? AND PROPERTY_ID = ?");
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		try
		{
			ps.setInt(1, tableNameId);
			ps.setLong(2, databaseId);
			Tools.logFine(ps);
			ps.execute();
		}
		finally
		{
			ps.close();
		}
	}

	/**
	 * Remove the internal reference label of a given object
	 * 
	 * @param ownerId the database id of the owner of the protection connection to delete.
	 * @param propertyId the database id of the protected id to delete
	 * @param propertyTableId the table id of the the property
	 * @param ownerTableId the table id of the owner
	 * @param cw
	 */
	public void unprotectObjectInternal(Integer ownerTableId, Long ownerId, Integer propertyTableId, Long propertyId,
			ConnectionWrapper cw) throws SQLException
	{
		StringBuilder statement = new StringBuilder(150);
		statement.append("DELETE FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE OWNER_TABLE = ? AND OWNER_ID = ? AND PROPERTY_TABLE = ? AND PROPERTY_ID = ?");
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		try
		{
			ps.setInt(1, ownerTableId);
			ps.setLong(2, ownerId);
			ps.setInt(3, propertyTableId);
			ps.setLong(4, propertyId);
			Tools.logFine(ps);
			ps.execute();
		}
		finally
		{
			ps.close();
		}

	}

	/**
	 * Remove all protection entries for a given class.
	 * 
	 * When this call returns successfully, no object has a property of the
	 * class corresponding to the given table name.
	 * 
	 * @param cw
	 * @param propertyTableId
	 *            the database table for which to remove all incoming
	 *            dependencies.
	 * @throws SQLException
	 */
	public void unprotectObjects(ConnectionWrapper cw, Integer propertyTableId) throws SQLException
	{
		StringBuilder statement = new StringBuilder(100);
		statement.append("DELETE FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE PROPERTY_TABLE = ? ");
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		try
		{
			ps.setInt(1, propertyTableId);
			Tools.logFine(ps);
			ps.execute();
		}
		finally
		{
			ps.close();
		}

	}

	/**
	 * Checks if a protection relationship exists before adding it. This is only
	 * necessary on updating objects.
	 * 
	 * @param ownerTableId
	 * @param ownerId
	 * @param propertyTableId
	 * @param propertyId
	 * @param classNameId
	 * @param cw
	 * @throws SQLException
	 */
	public void protectObjectInternalConditional(Integer ownerTableId, Long ownerId, String relationName,
			Integer propertyTableId, Long propertyId, Integer classNameId, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder statement = new StringBuilder(150);
		statement.append("SELECT COUNT(*) FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE PROPERTY_TABLE = ? AND PROPERTY_ID = ? AND OWNER_TABLE=? AND OWNER_ID=?");
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		try
		{
			ps.setInt(1, propertyTableId);
			ps.setLong(2, propertyId);
			ps.setInt(3, ownerTableId);
			ps.setLong(4, ownerId);
			Tools.logFine(ps);
			ResultSet rs = ps.executeQuery();
			// check if the query returns no results
			if (!rs.next() || rs.getLong(1)<=0)
			{
				protectObjectInternal(ownerTableId, ownerId, relationName, propertyTableId, propertyId, classNameId, cw);
			}
		}
		finally
		{
			ps.close();
		}
	}

}
