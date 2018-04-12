/*******************************************************************************
 *  
 * Copyright (c) 2009, 2018 Erik Berglund.
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
package com.github.conserveorm.test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import com.github.conserveorm.Persist;
import com.github.conserveorm.adapter.AdapterBase;
import com.github.conserveorm.connection.ConnectionWrapper;
import com.github.conserveorm.exceptions.SchemaPermissionException;
import com.github.conserveorm.tools.Defaults;
import com.github.conserveorm.tools.Tools;
import com.github.conserveorm.tools.generators.NameGenerator;
import com.github.conserveorm.tools.metadata.ObjectStack;

/**
 * Tools that are used during testing.
 * 
 * @author Erik Berglund
 * 
 */
public class TestTools
{

	private Persist persist;

	public TestTools(Persist p)
	{
		this.persist = p;
	}

	/**
	 * Change the name of oldClass to new newClass. All references will be
	 * updated.
	 * 
	 * @param oldClass
	 * @param newClass
	 * @throws SQLException
	 */
	public void changeName(Class<?> oldClass, Class<?> newClass) throws SQLException
	{
		ConnectionWrapper cw = persist.getConnectionWrapper();
		try
		{
			setTableName(oldClass, newClass, cw);
			cw.commitAndDiscard();
		}
		catch (Exception e)
		{
			// cancel the operation
			cw.rollbackAndDiscard();
			// re-throw the original exception
			throw new SQLException(e);
		}
	}

	/**
	 * Change the name of oldClass to new newClass. All references will be
	 * updated.
	 * 
	 * @param oldClass
	 * @param newClass
	 * @throws SQLException
	 */
	public void changeName(Class<?> oldClass, String newClassName, String newTableName, String newArrayMemberTable)
			throws SQLException
	{
		ConnectionWrapper cw = persist.getConnectionWrapper();
		try
		{
			setTableName(oldClass, newClassName, newTableName, newArrayMemberTable, cw);
			cw.commitAndDiscard();
		}
		catch (Exception e)
		{
			// cancel the operation
			cw.rollbackAndDiscard();
			// re-throw the original exception
			throw new SQLException(e);
		}
	}

	/**
	 * This method will fail if we do not have schema modification enabled.
	 * 
	 * @param oldClass
	 * @param newClass
	 * @throws SQLException
	 */
	public void setTableName(Class<?> oldClass, Class<?> newClass, ConnectionWrapper cw) throws SQLException
	{
		AdapterBase adapter = persist.getAdapter();
		String oldTableName = NameGenerator.getTableName(oldClass, adapter);
		String newTableName = NameGenerator.getTableName(newClass, adapter);
		String oldClassName = NameGenerator.getSystemicName(oldClass);
		String newClassName = NameGenerator.getSystemicName(newClass);

		String newArrayMemberTable = NameGenerator.getArrayMemberTableName(newClass, adapter);

		// check if the table names has changed
		if (!oldTableName.equals(newTableName))
		{
			Integer oldTableNameId = adapter.getPersist().getTableNameNumberMap().getNumber(cw, oldTableName);
			Integer newTableNameId = adapter.getPersist().getTableNameNumberMap().getNumber(cw,newTableName);
			// update the cache
			adapter.getPersist().getCache().purge(oldTableName);

			// alter the table name
			persist.getTableManager().setTableName(oldTableName,oldClass, newTableName,newClass, cw);

			// change the array tables
			updateAllRelations(NameGenerator.getArrayTablename(adapter), Defaults.COMPONENT_TABLE_COL, oldTableNameId, newTableNameId, cw);
			persist.getTableManager().setTableName(NameGenerator.getArrayMemberTableName(oldClass, adapter),oldClass,
					newArrayMemberTable,newClass, cw);

			// Update ownership relations
			updateAllRelations(Defaults.HAS_A_TABLENAME, "OWNER_TABLE", oldTableNameId, newTableNameId, cw);
			updateAllRelations(Defaults.HAS_A_TABLENAME, "PROPERTY_TABLE",oldTableNameId, newTableNameId, cw);

			// update type info table
			updateAllRelations(Defaults.TYPE_TABLENAME, "OWNER_TABLE", oldTableName, newTableName, cw);
			// update class name table
			updateAllRelations(Defaults.TABLE_NAME_TABLENAME, "TABLENAME", oldTableName, newTableName, cw);
			
			//update index list
			updateAllRelations(Defaults.INDEX_TABLENAME,"TABLE_NAME",oldTableName,newTableName,cw);
		}
		// check if the class name has changed
		if (!newClassName.equals(oldClassName))
		{
			Integer newClassNameId = adapter.getPersist().getClassNameNumberMap().getNumber(cw, newClassName);
			Integer oldClassNameId = adapter.getPersist().getClassNameNumberMap().getNumber(cw, oldClassName);
			// update inheritance relations
			updateISArelation(oldClassName, newClassName, "SUPERCLASS", cw);
			updateISArelation(oldClassName, newClassName, "SUBCLASS", cw);
			// update superclasses
			ObjectStack oldStack = new ObjectStack(adapter,oldClass);
			List<Class<?>> superClasses = oldStack.getSuperClasses(oldClass);
			for(Class<?>superClass:superClasses)
			{
				updateSuperClass(NameGenerator.getTableName(superClass, adapter), oldClassName, newClassName, cw);
			}
			ObjectStack nuStack = new ObjectStack(adapter,newClass);
			superClasses = nuStack.getSuperClasses(newClass);
			for(Class<?>superClass:superClasses)
			{
				updateSuperClass(NameGenerator.getTableName(superClass, adapter), oldClassName, newClassName, cw);
			}
			// update array tables
			updateAllRelations(NameGenerator.getArrayTablename(adapter), Defaults.COMPONENT_CLASS_COL, oldClassNameId, newClassNameId, cw);
			if (persist.getTableManager().tableExists(newArrayMemberTable, cw))
			{
				updateAllRelations(newArrayMemberTable, Defaults.COMPONENT_CLASS_COL, oldClassNameId, newClassNameId, cw);
				// update all C_ARRAY_MEMBER_<newName> entries
				updateAllRelations(newArrayMemberTable, Defaults.COMPONENT_CLASS_COL, oldClassNameId, newClassNameId, cw);
			}

			// Update ownership relations
			updateAllRelations(Defaults.HAS_A_TABLENAME, "PROPERTY_CLASS", oldClassNameId, newClassNameId, cw);
			updateAllRelations(Defaults.TABLE_NAME_TABLENAME, "CLASS", oldClassName, newClassName, cw);
		}
	}

	/**
	 * This method will fail if we do not have schema modification enabled.
	 * 
	 * @param oldClass
	 *            The class we're renaming
	 * @param newClassName
	 *            the name of the new class - should be on a format that lets it
	 *            be loaded by ClassLoader.loadClass(...).
	 * @param newTableName
	 *            the new name of the class' table.
	 * @param newArrayMemberTable
	 *            the name of the table to store array entries of this class.
	 * @param cw
	 *            a connection wrapper.
	 * @throws SQLException
	 */
	public void setTableName(Class<?> oldClass, String newClassName, String newTableName, String newArrayMemberTable,
			ConnectionWrapper cw) throws SQLException, SchemaPermissionException
	{
		AdapterBase adapter = persist.getAdapter();
		String oldTableName = NameGenerator.getTableName(oldClass, adapter);
		Integer oldTableNameId = adapter.getPersist().getTableNameNumberMap().getNumber(cw, oldTableName);
		Integer newTableNameId = adapter.getPersist().getTableNameNumberMap().getNumber(cw, newTableName);
		String oldClassName = NameGenerator.getSystemicName(oldClass);

		// check if the table names has changed
		if (!oldTableName.equals(newTableName))
		{
			// update the cache
			adapter.getPersist().getCache().purge(oldTableName);

			// alter the table name
			persist.getTableManager().setTableName(oldTableName,oldClass, newTableName,oldClass ,cw);

			// change the array tables
			updateAllRelations(NameGenerator.getArrayTablename(adapter), Defaults.COMPONENT_TABLE_COL, oldTableNameId, newTableNameId, cw);
			persist.getTableManager().setTableName(NameGenerator.getArrayMemberTableName(oldClass, adapter),oldClass,
					newArrayMemberTable,oldClass, cw);

			// Update ownership relations
			updateAllRelations(Defaults.HAS_A_TABLENAME, "OWNER_TABLE", 
					adapter.getPersist().getTableNameNumberMap().getNumber(cw, oldTableName),
					adapter.getPersist().getTableNameNumberMap().getNumber(cw,newTableName), cw);
			updateAllRelations(Defaults.HAS_A_TABLENAME, "PROPERTY_TABLE", 
					adapter.getPersist().getTableNameNumberMap().getNumber(cw, oldTableName),
					adapter.getPersist().getTableNameNumberMap().getNumber(cw,newTableName), cw);

			// update type info table
			updateAllRelations(Defaults.TYPE_TABLENAME, "OWNER_TABLE", oldTableName, newTableName, cw);

			// update tablename table
			updateAllRelations(Defaults.TABLE_NAME_TABLENAME, "TABLENAME", oldTableName, newTableName, cw);
			
			//update index list
			updateAllRelations(Defaults.INDEX_TABLENAME,"TABLE_NAME",oldTableName,newTableName,cw);
		}
		// check if the class name has changed
		if (!newClassName.equals(oldClassName))
		{
			//get the class name reference identifiers
			Integer oldClassNameId = adapter.getPersist().getClassNameNumberMap().getNumber(cw, oldClassName);
			Integer newClassNameId = adapter.getPersist().getClassNameNumberMap().getNumber(cw, newClassName);
			// update inheritance relations
			updateISArelation(oldClassName, newClassName, "SUPERCLASS", cw);
			updateISArelation(oldClassName, newClassName, "SUBCLASS", cw);
			
			// update superclasses
			// if the old superclass is different from the old
			Class<?> oldSuperClass = oldClass.getSuperclass();
			if (oldSuperClass != null)
			{
				updateSuperClass(NameGenerator.getTableName(oldSuperClass, adapter), oldClassName, newClassName, cw);
			}

			// update array tables
			updateAllRelations(NameGenerator.getArrayTablename(adapter), Defaults.COMPONENT_CLASS_COL, oldClassNameId, newClassNameId, cw);
			if (persist.getTableManager().tableExists(newArrayMemberTable, cw))
			{
				updateAllRelations(newArrayMemberTable, Defaults.COMPONENT_CLASS_COL, oldClassNameId, newClassNameId, cw);
				// update all C_ARRAY_MEMBER_<newName> entries
				updateAllRelations(newArrayMemberTable, Defaults.COMPONENT_CLASS_COL, oldClassNameId, newClassNameId, cw);
			}

			// Update ownership relations
			updateAllRelations(Defaults.HAS_A_TABLENAME, "PROPERTY_CLASS", 
					adapter.getPersist().getClassNameNumberMap().getNumber(cw, oldClassName) , 
					adapter.getPersist().getClassNameNumberMap().getNumber(cw, newClassName), cw);

			// update tablename table
			updateAllRelations(Defaults.TABLE_NAME_TABLENAME, "CLASS", oldClassName, newClassName, cw);
		}
	}

	/**
	 * Check superClassTableName for entries where C__REALCLASS is oldClassName,
	 * change it to newClassName.
	 * 
	 * @param newTableName
	 * @param oldClassName
	 * @param newClassName
	 * @throws SQLException
	 */
	private void updateSuperClass(String superClassTableName, String oldClassName, String newClassName,
			ConnectionWrapper cw) throws SQLException
	{
		if (persist.getTableManager().tableExists(superClassTableName, cw))
		{
			Integer oldClassNameId = persist.getClassNameNumberMap().getNumber(cw, oldClassName);
			Integer newClassNameId = persist.getClassNameNumberMap().getNumber(cw, newClassName);
			updateAllRelations(superClassTableName, Defaults.REAL_CLASS_COL, oldClassNameId, newClassNameId, cw);
		}
	}

	/**
	 * Change the SUPERCLASS/SUBCLASS entry for any of the affected rows.
	 * 
	 * @param oldClassName
	 * @param newClassName
	 * @throws SQLException
	 */
	private void updateISArelation(String oldClassName, String newClassName, String colName, ConnectionWrapper cw)
			throws SQLException
	{
		updateAllRelations(Defaults.IS_A_TABLENAME, colName, oldClassName, newClassName, cw);
	}

	/**
	 * Change all rows of table where column matches oldValue to newValue.
	 * 
	 * @param table
	 * @param column
	 * @param oldValue
	 * @param newValue
	 * @param cw
	 * @throws SQLException
	 */
	private void updateAllRelations(String table, String column, String oldValue, String newValue, ConnectionWrapper cw)
			throws SQLException
	{
		StringBuilder stmt = new StringBuilder("UPDATE ");
		stmt.append(table);
		stmt.append(" SET ");
		stmt.append(column);
		stmt.append(" = ? WHERE ");
		stmt.append(column);
		stmt.append(" = ?");
		PreparedStatement ps = cw.prepareStatement(stmt.toString());
		ps.setString(1, newValue);
		ps.setString(2, oldValue);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
	}
	/**
	 * Change all rows of table where column matches oldValue to newValue.
	 * 
	 * @param table
	 * @param column
	 * @param oldValue
	 * @param newValue
	 * @param cw
	 * @throws SQLException
	 */
	private void updateAllRelations(String table, String column, Integer oldValue, Integer newValue, ConnectionWrapper cw)
			throws SQLException
	{
		StringBuilder stmt = new StringBuilder("UPDATE ");
		stmt.append(table);
		stmt.append(" SET ");
		stmt.append(column);
		stmt.append(" = ? WHERE ");
		stmt.append(column);
		stmt.append(" = ?");
		PreparedStatement ps = cw.prepareStatement(stmt.toString());
		ps.setInt(1, newValue);
		ps.setInt(2, oldValue);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
	}
}
