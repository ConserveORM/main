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

/**
 * @author Erik Berglund
 * 
 */
public class ProtectionEntry
{
	private Long propertyId;
	private Integer propertyTableNameId;
	private Integer propertyClassNameId;
	private String relationName;

	/**
	 * Create a new protection entry for the given property.
	 * @param propertyTableNameId id of the table name of the property this entry protects.
	 * @param propertyId database ID of the property this entry protects.
	 */
	public ProtectionEntry(Integer propertyTableNameId, Long propertyId)
	{
		this(propertyTableNameId, (Integer) null, propertyId, (String) null);
	}


	public ProtectionEntry(Integer propertyTableNameId, Integer propertyClassNameId,
			Long propertyId, String relationName)
	{
		this.propertyTableNameId = propertyTableNameId;
		this.propertyClassNameId = propertyClassNameId;
		this.propertyId = propertyId;
		this.relationName = relationName;
	}

	public ProtectionEntry(ConnectionWrapper cw,Class<?> propertyClass, Long propertyId,
			String relationName, AdapterBase adapter) throws SQLException
	{
		this(adapter.getPersist().getTableNameNumberMap().getNumber(cw, propertyClass), 
				adapter.getPersist().getClassNameNumberMap().getNumber(cw, propertyClass), propertyId, relationName);
	}

	/**
	 * Save a HAS_A relationship for this object and the given owner object.
	 * 
	 * @param ownerTableId
	 *            the database table name of the owner object.
	 * @param ownerId
	 *            the database id of the owner object.
	 * @param protectionManager
	 *            the protection manager to use.
	 * @param cw
	 *            the connection for the current transaction.
	 * @throws SQLException
	 */
	public void save(Integer ownerTableId, long ownerId,
			ProtectionManager protectionManager, ConnectionWrapper cw)
			throws SQLException
	{
		protectionManager.protectObjectInternal(ownerTableId, ownerId,
				this.relationName, propertyTableNameId, propertyId,
				propertyClassNameId, cw);
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
			if ((this.propertyTableNameId == null && other.propertyTableNameId == null)
					|| (this.propertyTableNameId != null 
						&& (this.propertyTableNameId.equals(other.propertyTableNameId) 
							&& this.propertyId.equals(other.getPropertyId()))))
			{
				res = true;
			}
		}
		return res;
	}
	
	@Override
	public int hashCode()
	{
		int res = this.propertyId.intValue();
		res+=this.propertyTableNameId;
		if(this.propertyClassNameId!=null)
		{
			res+=this.propertyClassNameId;
		}
		if(this.relationName!=null)
		{
			res+=this.relationName.hashCode();
		}
		return res;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return propertyTableNameId + " (" + propertyId + ")";
	}

	public Long getPropertyId()
	{
		return propertyId;
	}

	public Integer getPropertyTableNameId()
	{
		return propertyTableNameId;
	}
	
}
