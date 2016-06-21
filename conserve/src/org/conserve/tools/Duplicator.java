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
package org.conserve.tools;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.conserve.Persist;
import org.conserve.connection.ConnectionWrapper;

/**
 * This class performs the actual copying required by the Persist.duplicate()
 * method.
 * 
 * @author Erik Berglund
 * 
 */
public class Duplicator
{

	private Persist target;
	private Persist source;
	private String sourceObjectTableName;
	private ConnectionWrapper sourceCw;

	public Duplicator(Persist source, Persist target)
	{
		this.source = source;
		this.target = target;
		sourceObjectTableName = NameGenerator.getTableName(Object.class,
				source.getAdapter());
	}

	public void doCopy() throws SQLException
	{
		sourceCw = source.getConnectionWrapper();
		ConnectionWrapper targetCw = target.getConnectionWrapper();
		// find the number of objects in the source database
		long count = getCount();
		// find the smallest object id in the source
		Long currentId = getSmallestId();
		// iterate over all object ids in the source
		try
		{
			// read the objects, one by one,
			// save each object
			for (long current = 0; current < count; current++)
			{
				if (currentId == null)
				{
					break;
				}
				Object currentObject = source.getObject(sourceCw, Object.class,
						currentId);
				if (!currentObject.getClass().isArray())
				{
					// only non-arrays are treated as proper objects.
					target.saveObjectUnprotected(targetCw, currentObject);
					copyExternalProtection(targetCw, currentObject);
					targetCw.commit();
				}
				// get the next id
				currentId = getSmallestIdLargerThan(currentId);
			}
		}
		catch (ClassNotFoundException e)
		{
			throw new SQLException(e);
		}
		catch (IOException e)
		{
			throw new SQLException(e);
		}
		finally
		{
			sourceCw.commitAndDiscard();
			targetCw.commitAndDiscard();
		}
	}

	/**
	 * Copy the external protection entry for currentObject from source to
	 * target it exists in source.
	 * 
	 * @param targetCw
	 * @param object
	 * @throws SQLException
	 */
	private void copyExternalProtection(ConnectionWrapper targetCw,
			Object object) throws SQLException
	{

		String sourceTableName = null;
		String targetTableName = null;
		if (object.getClass().isArray())
		{
			sourceTableName = NameGenerator.getArrayTablename(source.getAdapter());
			targetTableName = NameGenerator.getArrayTablename(target.getAdapter());
		}
		else
		{
			sourceTableName = NameGenerator.getTableName(object,
					source.getAdapter());
			targetTableName = NameGenerator.getTableName(object,
					target.getAdapter());
		}
		if (source.getProtectionManager().isProtectedExternal(sourceTableName,
				source.getId(object), sourceCw))
		{
			target.getProtectionManager().protectObjectExternal(
					targetTableName, target.getId(object),
					NameGenerator.getSystemicName(object.getClass()), targetCw);
		}

	}

	/**
	 * Get the smallest id larger than some limit value.
	 * 
	 * @param currentId
	 *            the limit value.
	 * @return the smallest id such that id>currentId, or null if no id is
	 *         found.
	 * @throws SQLException
	 */
	private Long getSmallestIdLargerThan(Long currentId) throws SQLException
	{
		StringBuilder statement = new StringBuilder(100);
		statement.append("SELECT ");
		statement.append(Defaults.ID_COL);
		statement.append(" FROM ");
		statement.append(sourceObjectTableName);
		statement.append(" WHERE ");
		statement.append(Defaults.ID_COL);
		statement.append(">");
		statement.append(currentId);
		statement.append(" ORDER BY ");
		statement.append(Defaults.ID_COL);
		statement.append(" ASC ");
		String limitString = source.getAdapter().getLimitString();
		limitString = limitString.replaceAll("\\?", "1");
		if (source.getAdapter().isPutLimitOffsetBeforeColumns())
		{
			statement.insert(7, limitString);
		}
		else
		{
			statement.append(limitString);
		}
		PreparedStatement ps = sourceCw.prepareStatement(statement.toString());
		try
		{
			ResultSet res = ps.executeQuery();
			if (res.next())
			{
				return res.getLong(1);
			}
			return null;
		}
		finally
		{
			ps.close();
		}
	}

	/**
	 * Get the smallest ID in the database.
	 * 
	 * @return the smallest id or null if the database is empty.
	 * 
	 * @throws SQLException
	 */
	private Long getSmallestId() throws SQLException
	{
		StringBuilder statement = new StringBuilder(100);
		statement.append("SELECT ");
		statement.append(Defaults.ID_COL);
		statement.append(" FROM ");
		statement.append(sourceObjectTableName);
		statement.append(" ORDER BY ");
		statement.append(Defaults.ID_COL);
		statement.append(" ASC ");
		String limitString = source.getAdapter().getLimitString();
		limitString = limitString.replaceAll("\\?", "1");
		if (source.getAdapter().isPutLimitOffsetBeforeColumns())
		{
			statement.insert(7, limitString);
		}
		else
		{
			statement.append(limitString);
		}
		PreparedStatement ps = sourceCw.prepareStatement(statement.toString());
		try
		{
			ResultSet res = ps.executeQuery();
			if (res.next())
			{
				return res.getLong(1);
			}
			return null;
		}
		finally
		{
			ps.close();
		}
	}

	/**
	 * Get the number of objects in the database.
	 * 
	 * @return
	 * @throws SQLException
	 */
	private long getCount() throws SQLException
	{
		StringBuilder statement = new StringBuilder(100);
		statement.append("SELECT COUNT(");
		statement.append(Defaults.ID_COL);
		statement.append(") FROM ");
		statement.append(sourceObjectTableName);
		PreparedStatement ps = sourceCw.prepareStatement(statement.toString());
		try
		{
			ResultSet res = ps.executeQuery();
			if (res.next())
			{
				return res.getLong(1);
			}
			return 0;
		}
		finally
		{
			ps.close();
		}
	}

}
