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
import org.conserve.connection.ConnectionWrapper;
import org.conserve.tools.Defaults;
import org.conserve.tools.Tools;

/**
 * Responsible for the reference-counting based protection scheme.
 * 
 * @author Erik Berglund
 * 
 */
public class ProtectionManager
{
	/**
	 * Check if an object, identified by tablename and id, is protected
	 * This checks references recursively because an object can contain itself.
	 * 
	 * @param tableName
	 * @param databaseId
	 * @param cw
	 * @return
	 * @throws SQLException
	 */
	public boolean isProtected(String tableName, Long databaseId, ConnectionWrapper cw) throws SQLException
	{
		DependentSet depSet = new DependentSet(tableName, databaseId, cw);
		return depSet.isProtected();
	}

	public boolean isProtectedExternal(String tableName, Long databaseId, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder statement = new StringBuilder(150);
		statement.append("SELECT * FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE OWNER_TABLE IS NULL AND OWNER_ID IS NULL AND PROPERTY_TABLE = ? AND PROPERTY_ID = ?");
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		ps.setString(1, tableName);
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
	 * @param tableName
	 * @param databaseId
	 * @param cw
	 * @throws SQLException
	 */
	public void protectObjectExternal(String tableName, Long databaseId, String className, ConnectionWrapper cw)
			throws SQLException
	{
		// ensure that the object is labelled as coming from outside
		PreparedStatement ps = cw.prepareStatement("INSERT INTO " + Defaults.HAS_A_TABLENAME
				+ " (PROPERTY_TABLE,PROPERTY_ID,PROPERTY_CLASS) values (?,?,?)");
		try
		{
			ps.setString(1, tableName);
			ps.setLong(2, databaseId);
			ps.setString(3, className);
			Tools.logFine(ps);
			ps.execute();
		}
		finally
		{
			ps.close();
		}
	}

	/**
	 * Create a protection entry for the given owner-property relationship.
	 * 
	 * @param ownerTableName
	 *            the table name of the owning object.
	 * @param ownerId
	 *            the database id of the owning object.
	 * @param relationName
	 *            the name the property has within the owner.
	 * @param propertyTableName
	 *            the table name of the owned object, or JAVA_LANG_OBJECT if the
	 *            class is an interface.
	 * @param propertyId
	 *            the database id of the owned object.
	 * @param propertyClass
	 *            the canonical name of the actual property class.
	 * @param cw
	 *            the connection to the database.
	 * @throws SQLException
	 */
	public void protectObjectInternal(String ownerTableName, Long ownerId, String relationName,
			String propertyTableName, Long propertyId, String propertyClass, ConnectionWrapper cw) throws SQLException
	{
		// ensure that the object is labelled as coming from inside
		PreparedStatement ps = cw.prepareStatement("INSERT INTO " + Defaults.HAS_A_TABLENAME
				+ " (OWNER_TABLE,OWNER_ID,PROPERTY_TABLE,PROPERTY_ID,PROPERTY_CLASS," + Defaults.RELATION_NAME_COL
				+ ") values (?,?,?,?,?,?)");
		try
		{
			ps.setString(1, ownerTableName);
			ps.setLong(2, ownerId);
			ps.setString(3, propertyTableName);
			ps.setLong(4, propertyId);
			if (propertyClass == null)
			{
				ps.setNull(5, java.sql.Types.VARCHAR);
			}
			else
			{
				ps.setString(5, propertyClass);
			}
			ps.setString(6, relationName);
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
	 * @param tableName
	 * @param databaseId
	 * @param cw
	 *            the connection object.
	 * 
	 * @throws SQLException
	 */
	public void unprotectObjectExternal(String tableName, Long databaseId, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder statement = new StringBuilder(150);
		statement.append("DELETE FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE OWNER_TABLE IS NULL AND PROPERTY_TABLE = ? AND PROPERTY_ID = ?");
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		try
		{
			ps.setString(1, tableName);
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
	 * @param cw
	 */
	public void unprotectObjectInternal(String ownerTable, Long ownerId, String propertyTable, Long propertyId,
			ConnectionWrapper cw) throws SQLException
	{
		StringBuilder statement = new StringBuilder(150);
		statement.append("DELETE FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE OWNER_TABLE = ? AND OWNER_ID = ? AND PROPERTY_TABLE = ? AND PROPERTY_ID = ?");
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		try
		{
			ps.setString(1, ownerTable);
			ps.setLong(2, ownerId);
			ps.setString(3, propertyTable);
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
	 * @param propertyTable
	 *            the database table for which to remove all incoming
	 *            dependencies.
	 * @throws SQLException
	 */
	public void unprotectObjects(ConnectionWrapper cw, String propertyTable) throws SQLException
	{
		StringBuilder statement = new StringBuilder(100);
		statement.append("DELETE FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE PROPERTY_TABLE = ? ");
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		try
		{
			ps.setString(1, propertyTable);
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
	 * @param ownerTable
	 * @param ownerId
	 * @param propertyTable
	 * @param propertyId
	 * @param canonicalName
	 * @param cw
	 * @throws SQLException
	 */
	public void protectObjectInternalConditional(String ownerTable, Long ownerId, String relationName,
			String propertyTable, Long propertyId, String canonicalName, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder statement = new StringBuilder(150);
		statement.append("SELECT COUNT(*) FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE PROPERTY_TABLE = ? AND PROPERTY_ID = ? AND OWNER_TABLE=? AND OWNER_ID=?");
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		try
		{
			ps.setString(1, propertyTable);
			ps.setLong(2, propertyId);
			ps.setString(3, ownerTable);
			ps.setLong(4, ownerId);
			Tools.logFine(ps);
			ResultSet rs = ps.executeQuery();
			// check if the query returns no results
			if (!rs.next() || rs.getLong(1)<=0)
			{
				protectObjectInternal(ownerTable, ownerId, relationName, propertyTable, propertyId, canonicalName, cw);
			}
		}
		finally
		{
			ps.close();
		}
	}

}
