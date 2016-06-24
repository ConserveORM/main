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

import java.sql.SQLException;

import org.conserve.adapter.AdapterBase;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.tools.NameGenerator;

/**
 * @author Erik Berglund
 * 
 */
public class ProtectionEntry
{
	private Long propertyId;
	private String propertyTableName;
	private String propertyClassName;
	private String relationName;

	/**
	 * Create a new protection entry for the given property.
	 * @param propertyTableName table name of the property this entry protects.
	 * @param propertyId database ID of the property this entry protects.
	 */
	public ProtectionEntry(String propertyTableName, Long propertyId)
	{
		this(propertyTableName, (String) null, propertyId, (String) null);
	}

	public ProtectionEntry(String propertyTableName, String propertyClassName,
			Long propertyId, String relationName)
	{
		this.propertyTableName = propertyTableName;
		this.propertyClassName = propertyClassName;
		this.propertyId = propertyId;
		this.relationName = relationName;
	}

	public ProtectionEntry(Class<?> propertyClass, Long propertyId,
			String relationName, AdapterBase adapter)
	{
		this(NameGenerator.getTableName(propertyClass, adapter), NameGenerator
				.getSystemicName(propertyClass), propertyId, relationName);
	}

	/**
	 * Save a HAS_A relationship for this object and the given owner object.
	 * 
	 * @param ownerTable
	 *            the database table name of the owner object.
	 * @param ownerId
	 *            the database id of the owner object.
	 * @param protectionManager
	 *            the protection manager to use.
	 * @param cw
	 *            the connection for the current transaction.
	 * @throws SQLException
	 */
	public void save(String ownerTable, long ownerId,
			ProtectionManager protectionManager, ConnectionWrapper cw)
			throws SQLException
	{
		protectionManager.protectObjectInternal(ownerTable, ownerId,
				this.relationName, propertyTableName, propertyId,
				propertyClassName, cw);
	}

	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj)
	{
		boolean res = false;
		if (obj instanceof ProtectionEntry)
		{
			ProtectionEntry other = (ProtectionEntry) obj;
			if ((this.propertyTableName == null && other.propertyTableName == null)
					|| (this.propertyTableName != null && (this.propertyTableName
							.equals(other.getPropertyTableName()) && this.propertyId
							.equals(other.getPropertyId()))))
			{
				res = true;
			}
		}
		return res;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return propertyTableName + " (" + propertyId + ")";
	}

	public Long getPropertyId()
	{
		return propertyId;
	}

	public String getPropertyTableName()
	{
		return propertyTableName;
	}
}
