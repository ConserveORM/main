/*******************************************************************************
 *  
 * Copyright (c) 2009, 2019 Erik Berglund.
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
import java.sql.SQLException;

import com.github.conserveorm.adapter.AdapterBase;
import com.github.conserveorm.connection.ConnectionWrapper;
import com.github.conserveorm.tools.generators.NameGenerator;
import com.github.conserveorm.tools.protection.ProtectionManager;

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
		String arrayTableName = NameGenerator.getArrayTablename(adapter);
		// iterate over all the elements
		for (int x = 0; x < length; x++)
		{
			PreparedStatement ps = cw.prepareStatement(statement);
			ps.setLong(1, arrayId);
			ps.setInt(2, x);
			Object value = Array.get(array, x);
			if (ObjectTools.isDatabasePrimitive(compType))
			{
				// The array entry is a primitive type, add it directly
				Tools.setParameter(ps, compType, 3, value,adapter);
				Integer compTypeNameId = adapter.getPersist().getClassNameNumberMap().getNumber(cw, compType);
				ps.setInt(4, compTypeNameId);
				Tools.logFine(ps);
				ps.execute();
				ps.close();

			}
			else
			{
				// this is another type of object, insert a reference
				ps.setInt(4, adapter.getPersist().getClassNameNumberMap().getNumber(cw,value.getClass()));
				Long valueId = adapter.getPersist().saveObjectUnprotected(cw,
						value, delayBuffer);
				Long memberId = null;
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
					delayBuffer.addArrayEntry(tableName, Defaults.VALUE_COL, memberId,
							value, compType,System.identityHashCode(array),arrayTableName,arrayId);
				}
				else
				{
					ps.setLong(3, valueId);
					Tools.logFine(ps);
					ps.execute();
					ps.close();
					// get the new id of the __ARRAY_MEMBER entry
					memberId = adapter.getPersist().getLastId(cw, tableName);
				}

				String valueClassName = null;
				String propertyName = arrayTableName;
				// add a protection entry for the __ARRAY table
				if (!compType.isArray())
				{
					valueClassName = NameGenerator.getSystemicName(value
							.getClass());
					propertyName = NameGenerator
							.getTableName(compType, adapter);
				}
				if (valueId != null)
				{
					// protect the object with array table as owner
					protectionManager
							.protectObjectInternal(arrayTableName, arrayId,null,
									propertyName, valueId, valueClassName, cw);
				}

			}
		}
	}
}
