package org.conserve.test;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.conserve.Persist;
import org.conserve.adapter.AdapterBase;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.exceptions.SchemaPermissionException;
import org.conserve.tools.Defaults;
import org.conserve.tools.NameGenerator;
import org.conserve.tools.Tools;

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
			// update the cache
			adapter.getPersist().getCache().purge(oldTableName);

			// alter the table name
			persist.getTableManager().setTableName(oldTableName, newTableName, cw);

			// change the array tables
			updateAllRelations(Defaults.ARRAY_TABLENAME, Defaults.COMPONENT_TABLE_COL, oldTableName, newTableName, cw);
			persist.getTableManager().setTableName(NameGenerator.getArrayMemberTableName(oldClass, adapter),
					newArrayMemberTable, cw);

			// Update ownership relations
			updateAllRelations(Defaults.HAS_A_TABLENAME, "OWNER_TABLE", oldTableName, newTableName, cw);
			updateAllRelations(Defaults.HAS_A_TABLENAME, "PROPERTY_TABLE", oldTableName, newTableName, cw);

			// update type info table
			updateAllRelations(Defaults.TYPE_TABLENAME, "OWNER_TABLE", oldTableName, newTableName, cw);
		}
		// check if the class name has changed
		if (!newClassName.equals(oldClassName))
		{
			// update inheritance relations
			updateISArelation(oldClassName, newClassName, "SUPERCLASS", cw);
			updateISArelation(oldClassName, newClassName, "SUBCLASS", cw);
			// update superclasses
			Class<?> superClass = newClass.getSuperclass();
			if (superClass != null)
			{
				updateSuperClass(NameGenerator.getTableName(superClass, adapter), oldClassName, newClassName, cw);
				// if the old superclass is different from the old
				Class<?> oldSuperClass = oldClass.getSuperclass();
				if (oldSuperClass != null && !oldSuperClass.equals(superClass))
				{
					updateSuperClass(NameGenerator.getTableName(oldSuperClass, adapter), oldClassName, newClassName, cw);
				}
			}
			// update array tables
			updateAllRelations(Defaults.ARRAY_TABLENAME, Defaults.COMPONENT_CLASS_COL, oldClassName, newClassName, cw);
			if (persist.getTableManager().tableExists(newArrayMemberTable, cw))
			{
				updateAllRelations(newArrayMemberTable, Defaults.COMPONENT_CLASS_COL, oldClassName, newClassName, cw);
				// update all C_ARRAY_MEMBER_<newName> entries
				updateAllRelations(newArrayMemberTable, Defaults.COMPONENT_CLASS_COL, oldClassName, newClassName, cw);
			}

			// Update ownership relations
			updateAllRelations(Defaults.HAS_A_TABLENAME, "PROPERTY_CLASS", oldClassName, newClassName, cw);
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
		String oldClassName = NameGenerator.getSystemicName(oldClass);

		// check if the table names has changed
		if (!oldTableName.equals(newTableName))
		{
			// update the cache
			adapter.getPersist().getCache().purge(oldTableName);

			// alter the table name
			persist.getTableManager().setTableName(oldTableName, newTableName, cw);

			// change the array tables
			updateAllRelations(Defaults.ARRAY_TABLENAME, Defaults.COMPONENT_TABLE_COL, oldTableName, newTableName, cw);
			persist.getTableManager().setTableName(NameGenerator.getArrayMemberTableName(oldClass, adapter), newArrayMemberTable, cw);

			// Update ownership relations
			updateAllRelations(Defaults.HAS_A_TABLENAME, "OWNER_TABLE", oldTableName, newTableName, cw);
			updateAllRelations(Defaults.HAS_A_TABLENAME, "PROPERTY_TABLE", oldTableName, newTableName, cw);

			// update type info table
			updateAllRelations(Defaults.TYPE_TABLENAME, "OWNER_TABLE", oldTableName, newTableName, cw);
		}
		// check if the class name has changed
		if (!newClassName.equals(oldClassName))
		{
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
			updateAllRelations(Defaults.ARRAY_TABLENAME, Defaults.COMPONENT_CLASS_COL, oldClassName, newClassName, cw);
			if (persist.getTableManager().tableExists(newArrayMemberTable, cw))
			{
				updateAllRelations(newArrayMemberTable, Defaults.COMPONENT_CLASS_COL, oldClassName, newClassName, cw);
				// update all C_ARRAY_MEMBER_<newName> entries
				updateAllRelations(newArrayMemberTable, Defaults.COMPONENT_CLASS_COL, oldClassName, newClassName, cw);
			}

			// Update ownership relations
			updateAllRelations(Defaults.HAS_A_TABLENAME, "PROPERTY_CLASS", oldClassName, newClassName, cw);
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
			updateAllRelations(superClassTableName, Defaults.REAL_CLASS_COL, oldClassName, newClassName, cw);
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
}
