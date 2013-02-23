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
package org.conserve.tools;

import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.conserve.adapter.AdapterBase;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.tools.protection.ProtectionManager;

/**
 * Saves the entries of a given array, and assigns the proper protection.
 * 
 * @author Erik Berglund
 * 
 */
public class ArrayEntryWriter
{

	private AdapterBase adapter;

	public ArrayEntryWriter(AdapterBase adapter)
	{
		this.adapter = adapter;
	}

	/**
	 * Add the elements of an array.
	 * 
	 * The new elements will be marked as members of the most recently created
	 * entity (an entry in the __ARRAY table).
	 * 
	 * @param cw
	 *            the connection wrapper to use.
	 * @param arrayId
	 *            the database id of the array to save entries for.
	 * @param array
	 *            the array to save.
	 * @throws SQLException
	 * @throws IOException
	 */
	public void addArrayEntries(ConnectionWrapper cw, Long arrayId,
			Object array, DelayedInsertionBuffer delayBuffer)
			throws SQLException
	{
		ProtectionManager protectionManager = adapter.getPersist()
				.getProtectionManager();
		Class<?> compType = array.getClass().getComponentType();
		// the table name for the relation entries
		String tableName = NameGenerator.getArrayMemberTableName(compType,
				adapter);
		// create the statement
		StringBuilder builder = new StringBuilder("INSERT INTO ");
		builder.append(tableName);
		builder.append("(");
		builder.append(Defaults.ARRAY_MEMBER_ID);
		builder.append(",");
		builder.append(Defaults.ARRAY_POSITION);
		builder.append(",");
		builder.append(Defaults.VALUE_COL);
		builder.append(",");
		builder.append(Defaults.COMPONENT_CLASS_COL);
		builder.append(")values(?,?,?,?)");
		String statement = builder.toString();
		int length = Array.getLength(array);
		// iterate over all the elements
		for (int x = 0; x < length; x++)
		{
			PreparedStatement ps = cw.prepareStatement(statement);
			ps.setLong(1, arrayId);
			ps.setInt(2, x);
			Object value = Array.get(array, x);
			if (ObjectTools.isPrimitive(compType))
			{
				// The array entry is a primitive type, add it directly
				Tools.setParameter(ps, compType, 3, value);
				ps.setString(4, ObjectTools.getSystemicName(compType));
				Tools.logFine(ps);
				ps.execute();
				ps.close();
				// get the new id of the __ARRAY_MEMBER entry
				long memberId = adapter.getPersist().getLastId(cw, tableName);
				// add a protection entry for the __ARRAY table
				protectionManager.protectObjectInternal(
						Defaults.ARRAY_TABLENAME, arrayId,null, NameGenerator
								.getArrayMemberTableName(compType, adapter),
						memberId, ObjectTools.getSystemicName(Array.get(array,
								x).getClass()), cw);

			}
			else
			{
				// this is another type of object, insert a reference
				ps.setString(4, ObjectTools.getSystemicName(value.getClass()));
				Long valueId = adapter.getPersist().saveObjectUnprotected(cw,
						value, delayBuffer);
				long memberId = 0;
				if (valueId == null)
				{
					// This means the object exists, either independently
					// or as part of another array.
					ps.setNull(3, java.sql.Types.BIGINT);
					Tools.logFine(ps);
					// save the object
					ps.execute();
					ps.close();
					// get the new id of the __ARRAY_MEMBER entry
					memberId = adapter.getPersist().getLastId(cw, tableName);
					// this is a circularly referenced object
					// mark it for later insertion
					delayBuffer.add(tableName, Defaults.VALUE_COL, memberId,
							value, compType,System.identityHashCode(array));
				}
				else
				{
					ps.setLong(
							3,
							adapter.getPersist().getCastId(compType,
									value.getClass(), valueId, cw));
					Tools.logFine(ps);

					// save the object
					ps.execute();
					ps.close();
					// get the new id of the __ARRAY_MEMBER entry
					memberId = adapter.getPersist().getLastId(cw, tableName);
				}

				String valueClassName = null;
				String propertyName = Defaults.ARRAY_TABLENAME;
				// add a protection entry for the __ARRAY table
				if (!compType.isArray())
				{
					valueClassName = ObjectTools.getSystemicName(value
							.getClass());
					propertyName = NameGenerator
							.getTableName(compType, adapter);
				}
				if (valueId != null)
				{
					// protect the object with array-member table as owner
					protectionManager
							.protectObjectInternal(tableName, memberId,null,
									propertyName, valueId, valueClassName, cw);
				}
				// protect the array-member with array as owner
				protectionManager.protectObjectInternal(
						Defaults.ARRAY_TABLENAME, arrayId, null,tableName,
						memberId, valueClassName, cw);

			}
		}
	}
}
