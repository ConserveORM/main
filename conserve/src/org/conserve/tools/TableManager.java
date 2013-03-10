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

import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.conserve.adapter.AdapterBase;
import org.conserve.annotations.AsBlob;
import org.conserve.annotations.AsClob;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.connection.DataConnectionPool;
import org.conserve.exceptions.IntegrityException;
import org.conserve.exceptions.SchemaPermissionException;
import org.conserve.select.All;
import org.conserve.select.StatementPrototypeGenerator;
import org.conserve.tools.generators.IdStatementGenerator;
import org.conserve.tools.generators.RelationDescriptor;
import org.conserve.tools.generators.SubclassMover;
import org.conserve.tools.metadata.ChangeDescription;
import org.conserve.tools.metadata.ConcreteObjectRepresentation;
import org.conserve.tools.metadata.DatabaseObjectRepresentation;
import org.conserve.tools.metadata.InheritanceModel;
import org.conserve.tools.metadata.MetadataException;
import org.conserve.tools.metadata.ObjectRepresentation;
import org.conserve.tools.metadata.ObjectStack;
import org.conserve.tools.protection.ProtectionManager;
import org.conserve.tools.uniqueid.UniqueIdGenerator;
import org.conserve.tools.uniqueid.UniqueIdTree;

/**
 * This object is responsible for creating tables and checking if tables exist.
 * 
 * @author Erik Berglund
 * 
 */
public class TableManager
{
	private int schemaTypeVersion = 1;
	private boolean createSchema;
	private DataConnectionPool connectionPool;
	private AdapterBase adapter;
	private ArrayList<Class<?>> existingClasses = new ArrayList<Class<?>>();

	public TableManager(boolean createSchema, DataConnectionPool connectionPool, AdapterBase adapter)
	{
		this.adapter = adapter;
		this.connectionPool = connectionPool;
		setCreateSchema(createSchema);
	}

	/**
	 * Create the system tables Conserve depends on.
	 * 
	 * @throws SQLException
	 * @throws SchemaPermissionException
	 */
	public void initializeSystemTables() throws SQLException, SchemaPermissionException
	{
		// check if the system tables exist
		ConnectionWrapper cw = connectionPool.getConnectionWrapper();
		try
		{
			int existingSchema = 0;
			// check if the version table exist
			if (tableExists(Defaults.SCHEMA_VERSION_TABLENAME, cw))
			{
				// get the existing schema version
				String query = "SELECT VERSION FROM " + Defaults.SCHEMA_VERSION_TABLENAME;
				PreparedStatement ps = cw.prepareStatement(query);
				Tools.logFine(ps);
				ResultSet rs = ps.executeQuery();
				if (rs.next())
				{
					existingSchema = rs.getInt(1);
				}
				rs.close();
				ps.close();

			}
			else
			{
				String createString = "CREATE TABLE " + Defaults.SCHEMA_VERSION_TABLENAME + " (VERSION "
						+ adapter.getIntegerTypeKeyword() + ")";
				PreparedStatement ps = cw.prepareStatement(createString);
				Tools.logFine(ps);
				ps.execute();
				ps.close();
				if (adapter.isRequiresCommitAfterTableCreation())
				{
					cw.commit();
				}
				// insert the current version
				String commandString = "INSERT INTO  " + Defaults.SCHEMA_VERSION_TABLENAME + "  (VERSION ) values (?)";
				ps = cw.prepareStatement(commandString);
				ps.setInt(1, schemaTypeVersion);
				Tools.logFine(ps);
				ps.execute();
				ps.close();
			}
			if (existingSchema < schemaTypeVersion)
			{
				upgradeSchema(existingSchema, cw);
			}
			else if (existingSchema > schemaTypeVersion)
			{
				throw new SQLException("Database schema is version " + existingSchema + " but Conserve is version "
						+ schemaTypeVersion);
			}
			if (!tableExists(Defaults.IS_A_TABLENAME, cw))
			{
				if (!this.createSchema)
				{
					throw new SchemaPermissionException(Defaults.IS_A_TABLENAME
							+ " does not exist, but can't create it.");
				}
				String createString = "CREATE TABLE " + Defaults.IS_A_TABLENAME + " (SUPERCLASS "
						+ adapter.getVarCharIndexed() + ",SUBCLASS " + adapter.getVarCharIndexed() + ")";
				PreparedStatement ps = cw.prepareStatement(createString);
				Tools.logFine(ps);
				ps.execute();
				ps.close();
				// create an index on the superclass name, since this is the
				// one we
				// will be searching for most frequently
				String commandString = "CREATE INDEX " + Defaults.IS_A_TABLENAME + "_SUPERCLASS_INDEX on "
						+ Defaults.IS_A_TABLENAME + "(SUPERCLASS" + adapter.getKeyLength() + ")";
				ps = cw.prepareStatement(commandString);
				Tools.logFine(ps);
				ps.execute();
				ps.close();
			}
			if (!tableExists(Defaults.HAS_A_TABLENAME, cw))
			{
				if (!this.createSchema)
				{
					throw new SchemaPermissionException(Defaults.HAS_A_TABLENAME
							+ " does not exist, but can't create it.");
				}
				String commandString = "CREATE TABLE " + Defaults.HAS_A_TABLENAME + " (OWNER_TABLE "
						+ adapter.getVarCharIndexed() + ", OWNER_ID " + adapter.getLongTypeKeyword() + ", "
						+ Defaults.RELATION_NAME_COL + " " + adapter.getVarCharKeyword() + ", PROPERTY_TABLE "
						+ adapter.getVarCharIndexed() + ", PROPERTY_ID " + adapter.getLongTypeKeyword()
						+ ", PROPERTY_CLASS " + adapter.getVarCharIndexed() + ")";
				PreparedStatement ps = cw.prepareStatement(commandString);
				Tools.logFine(ps);
				ps.execute();
				ps.close();
				// create an index on the tablename/id combinations, since
				// this is
				// the one we
				// will be searching for most frequently
				commandString = "CREATE INDEX " + Defaults.HAS_A_TABLENAME + "_OWNER_INDEX on "
						+ Defaults.HAS_A_TABLENAME + "(OWNER_TABLE" + adapter.getKeyLength() + ",OWNER_ID)";
				ps = cw.prepareStatement(commandString);
				Tools.logFine(ps);
				ps.execute();
				ps.close();

				commandString = "CREATE INDEX " + Defaults.HAS_A_TABLENAME + "_PROPERTY_INDEX on "
						+ Defaults.HAS_A_TABLENAME + "(PROPERTY_TABLE" + adapter.getKeyLength() + ",PROPERTY_ID)";
				ps = cw.prepareStatement(commandString);
				Tools.logFine(ps);
				ps.execute();
				ps.close();
			}
			if (!tableExists(Defaults.ARRAY_TABLENAME, cw))
			{
				if (!this.createSchema)
				{
					throw new SchemaPermissionException(Defaults.ARRAY_TABLENAME
							+ " does not exist, but can't create it.");
				}
				if (adapter.isSupportsIdentity())
				{
					PreparedStatement ps = cw

					.prepareStatement("CREATE TABLE " + Defaults.ARRAY_TABLENAME + " (" + Defaults.ID_COL + " "
							+ adapter.getIdentity() + " PRIMARY KEY, " + Defaults.COMPONENT_TABLE_COL + " "
							+ adapter.getVarCharIndexed() + ", " + Defaults.COMPONENT_CLASS_COL + " "
							+ adapter.getVarCharIndexed() + " )");

					Tools.logFine(ps);
					ps.execute();
					ps.close();
				}
				else
				{
					// the adapter does not support identity
					// check if we can use a trigger
					if (adapter.isSupportsTriggers())
					{
						// create the table as usual
						PreparedStatement ps = cw.prepareStatement("CREATE TABLE " + Defaults.ARRAY_TABLENAME + " ("
								+ Defaults.ID_COL + " " + adapter.getLongTypeKeyword() + " PRIMARY KEY, "
								+ Defaults.COMPONENT_TABLE_COL + " " + adapter.getVarCharIndexed() + ", "
								+ Defaults.COMPONENT_CLASS_COL + " " + adapter.getVarCharIndexed() + " )");

						Tools.logFine(ps);
						ps.execute();
						ps.close();
						createTriggeredSequence(cw, Defaults.ARRAY_TABLENAME);

					}
					else
					{
						throw new RuntimeException(
								"Database engines without both autoincrements and triggers are not supported at this time.");
					}
				}
				// create an index on the id, as this is the one we
				// will be searching for most frequently
				String commandString = "CREATE INDEX " + Defaults.ARRAY_TABLENAME + "_INDEX on "
						+ Defaults.ARRAY_TABLENAME + "(" + Defaults.ID_COL + ")";
				PreparedStatement ps = cw.prepareStatement(commandString);
				Tools.logFine(ps);
				ps.execute();
				ps.close();

			}

			if (!tableExists(Defaults.ARRAY_MEMBER_TABLE_NAME_ARRAY, cw))
			{
				if (!this.createSchema)
				{
					throw new SchemaPermissionException(Defaults.ARRAY_MEMBER_TABLE_NAME_ARRAY
							+ " does not exist, but can't create it.");
				}
				if (adapter.isSupportsIdentity())
				{
					StringBuilder create = new StringBuilder("CREATE TABLE ");
					create.append(Defaults.ARRAY_MEMBER_TABLE_NAME_ARRAY);
					create.append("(");
					create.append(Defaults.ID_COL);
					create.append(" " + adapter.getIdentity() + " PRIMARY KEY, ");
					create.append(Defaults.ARRAY_POSITION);
					create.append(" INT, ");
					create.append(Defaults.COMPONENT_CLASS_COL);
					create.append(" ");
					create.append(adapter.getVarCharIndexed());
					create.append(", ");
					create.append(Defaults.VALUE_COL);
					create.append(" ");
					create.append(adapter.getLongTypeKeyword());
					create.append(",");
					create.append(Defaults.ARRAY_MEMBER_ID);
					create.append(" ");
					create.append(adapter.getLongTypeKeyword());
					create.append(", FOREIGN KEY(");
					create.append(Defaults.ARRAY_MEMBER_ID);
					create.append(") REFERENCES ");
					create.append(Defaults.ARRAY_TABLENAME);
					create.append("(");
					create.append(Defaults.ID_COL);
					create.append("))");

					PreparedStatement ps = cw.prepareStatement(create.toString());
					Tools.logFine(ps);
					ps.execute();
					ps.close();
				}
				else
				{

					// the adapter does not support identity
					// check if we can use a trigger
					if (adapter.isSupportsTriggers())
					{
						// create the table as usual
						StringBuilder create = new StringBuilder("CREATE TABLE ");
						create.append(Defaults.ARRAY_MEMBER_TABLE_NAME_ARRAY);
						create.append("(");
						create.append(Defaults.ID_COL);
						create.append(" ");
						create.append(adapter.getLongTypeKeyword());
						create.append(" PRIMARY KEY, ");
						create.append(Defaults.ARRAY_POSITION);
						create.append(" INT, ");
						create.append(Defaults.COMPONENT_CLASS_COL);
						create.append(" ");
						create.append(adapter.getVarCharIndexed());
						create.append(", ");
						create.append(Defaults.VALUE_COL);
						create.append(" ");
						create.append(adapter.getLongTypeKeyword());
						create.append(",");
						create.append(Defaults.ARRAY_MEMBER_ID);
						create.append(" ");
						create.append(adapter.getLongTypeKeyword());
						create.append(", FOREIGN KEY(");
						create.append(Defaults.ARRAY_MEMBER_ID);
						create.append(") REFERENCES ");
						create.append(Defaults.ARRAY_TABLENAME);
						create.append("(");
						create.append(Defaults.ID_COL);
						create.append("))");
						String createString = create.toString();
						PreparedStatement ps = cw.prepareStatement(createString);
						Tools.logFine(ps);
						ps.execute();
						ps.close();

						// create the triggered sequence
						createTriggeredSequence(cw, Defaults.ARRAY_MEMBER_TABLE_NAME_ARRAY);

					}
					else
					{
						throw new RuntimeException(
								"Database engines without both autoincrements and triggers are not supported at this time.");
					}

				}
			}
			
			if(!tableExists(Defaults.TYPE_TABLENAME, cw))
			{
				//create the type table

				String createString = "CREATE TABLE " + Defaults.TYPE_TABLENAME + " (OWNER_TABLE "
						+ adapter.getVarCharIndexed() + ",COLUMN_NAME " + adapter.getVarCharIndexed() + ",COLUMN_CLASS " + adapter.getVarCharIndexed() +")";
				PreparedStatement ps = cw.prepareStatement(createString);
				Tools.logFine(ps);
				ps.execute();
				ps.close();
				
			}

			// commit, return connection to pool
			cw.commitAndDiscard();
		}
		catch (SQLException e)
		{
			cw.rollbackAndDiscard();
			throw e;
		}
	}

	/**
	 * @param existingSchema
	 * @throws SQLException
	 */
	private void upgradeSchema(int existingSchema, ConnectionWrapper cw) throws SQLException
	{

		// alter the schema
		if (existingSchema <= 0)
		{
			// update schema from version 0 to version 1

			// C__ARRAY
			if (tableExists(Defaults.ARRAY_TABLENAME, cw))
			{
				// rename COMPONENT_TYPE TO COMPONENT_TABLE
				this.renameColumn(Defaults.ARRAY_TABLENAME, "COMPONENT_TYPE", Defaults.COMPONENT_TABLE_COL, cw);
				// rename COMPONENT_CLASS_NAME TO COMPONENT_TYPE
				this.renameColumn(Defaults.ARRAY_TABLENAME, "COMPONENT_CLASS_NAME", Defaults.COMPONENT_CLASS_COL, cw);
			}
			// C__HAS_A
			if (tableExists(Defaults.HAS_A_TABLENAME, cw))
			{
				// add RELATION_NAME column
				this.createColumn(Defaults.HAS_A_TABLENAME, Defaults.RELATION_NAME_COL, adapter.getVarCharKeyword(), cw);
				// get a map of all classes
				List<Class<?>> classList = this.populateClassList(cw);
				HashMap<String, Class<?>> tableNameMap = new HashMap<String, Class<?>>();
				for (Class<?> c : classList)
				{
					String tableName = NameGenerator.getTableName(c, adapter);
					tableNameMap.put(tableName, c);
				}

				// for each entry in C__HAS_A with a null RELATION_NAME and a
				// non-null OWNER_TABLE, do the following:

				String commandString = "SELECT * FROM " + Defaults.HAS_A_TABLENAME + " WHERE "
						+ Defaults.RELATION_NAME_COL + " IS NULL AND OWNER_TABLE IS NOT NULL";
				PreparedStatement ps = cw.prepareStatement(commandString);
				Tools.logFine(ps);
				ResultSet rs = ps.executeQuery();
				while (rs.next())
				{
					// OWNER_TABLE does not start with C__ARRAY? if so, do this:
					String ownerTable = rs.getString("OWNER_TABLE");
					if (!ownerTable.startsWith("C__ARRAY"))
					{
						// find the owner class
						Class<?> ownerClass = tableNameMap.get(ownerTable);
						if (ownerClass != null)
						{
							// TODO: Implement this
							// for each property of the owner class that has
							// matching type,
							// find the matching id

							// update the RELATION_NAME for the id
						}
					}
				}
			}
		}

		// save the current version
		String commandString = "UPDATE  " + Defaults.SCHEMA_VERSION_TABLENAME + " SET VERSION = ?";
		PreparedStatement ps = cw.prepareStatement(commandString);
		ps.setInt(1, schemaTypeVersion);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
	}

	/**
	 * Create a sequence for the ID of the named table.
	 * 
	 * @param tableName
	 *            the name of the table to create a trigger for.
	 * @throws SQLException
	 */
	private void createTriggeredSequence(ConnectionWrapper cw, String tableName) throws SQLException
	{

		// create a thread-safe sequence
		String sequenceName = Tools.getSequenceName(tableName, adapter);
		String createGenerator = "CREATE GENERATOR " + sequenceName;
		PreparedStatement ps = cw.prepareStatement(createGenerator);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
		// create a trigger that updates on insert of the desired table
		String triggerName = Tools.getTriggerName(tableName, adapter);
		String toExectue = "CREATE TRIGGER " + triggerName + " FOR " + tableName + " ACTIVE BEFORE INSERT POSITION 0\n"
				+ "AS\n" + "BEGIN \n" + "if (NEW." + Defaults.ID_COL + " is NULL) then NEW." + Defaults.ID_COL
				+ " = GEN_ID(" + sequenceName + ", 1);\n" + "RDB$SET_CONTEXT('USER_SESSION', 'LAST__INSERT__ID', new."
				+ Defaults.ID_COL + ");\n" + "END";
		ps = cw.prepareStatement(toExectue);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
	}

	public void setCreateSchema(boolean createSchema)
	{
		this.createSchema = createSchema;
	}

	/**
	 * Check whether a given table exists.
	 * 
	 * @param objRes
	 *            .getTableName() the name of the database table.
	 */
	private boolean tableExists(ObjectRepresentation objRes, ConnectionWrapper cw) throws SQLException
	{
		return tableExists(objRes.getTableName(), cw);
	}

	public boolean tableExists(Class<?> clazz, ConnectionWrapper cw) throws SQLException
	{
		if (clazz.isArray())
		{
			return tableExists(NameGenerator.getArrayMemberTableName(clazz.getComponentType(), adapter), cw);
		}
		else
		{
			return tableExists(NameGenerator.getTableName(clazz, adapter), cw);
		}
	}

	/**
	 * Check if a table has a column with the desired name.
	 * 
	 * @param tableName
	 *            the table to check.
	 * @param columnName
	 *            the column to check.
	 * @param cw
	 * @return true if the table has this column, false otherwise.
	 * @throws SQLException
	 */
	private boolean columnExists(String tableName, String columnName, ConnectionWrapper cw) throws SQLException
	{
		if (adapter.tableNamesAreLowerCase())
		{
			tableName = tableName.toLowerCase();
		}
		Connection c = cw.getConnection();
		DatabaseMetaData metaData = c.getMetaData();
		ResultSet rs = metaData.getColumns(c.getCatalog(), null, tableName, columnName);
		boolean res = false;
		if (rs.next())
		{
			res = true;
			if (rs.next())
			{
				throw new SQLException("Multiple results found for table " + tableName + " and column " + columnName);
			}
		}
		return res;
	}

	/**
	 * Check if the named table exists.
	 * 
	 * @param tableName
	 * @return true if the table exists, false otherwise.
	 * @throws SQLException
	 */
	public boolean tableExists(String tableName, ConnectionWrapper cw) throws SQLException
	{
		if (adapter.tableNamesAreLowerCase())
		{
			tableName = tableName.toLowerCase();
		}
		Connection c = cw.getConnection();
		DatabaseMetaData metaData = c.getMetaData();
		ResultSet rs = metaData.getTables(c.getCatalog(), null, tableName, new String[] { "TABLE" });
		boolean res = false;
		if (rs.next())
		{
			res = true;
			if (rs.next())
			{
				throw new SQLException("Multiple results found for table " + tableName);
			}
		}
		return res;
	} 

	private void createTable(ConcreteObjectRepresentation objRes, ConnectionWrapper cw) throws SQLException,
			SchemaPermissionException
	{
		if (!this.createSchema)
		{
			throw new SchemaPermissionException(objRes.getTableName() + " does not exist, but can't be created.");
		}
		String createStatement = objRes.getTableCreationStatement(cw);

		PreparedStatement ps = cw.prepareStatement(createStatement);
		Tools.logFine(ps);
		ps.execute();
		ps.close();

		if (!adapter.isSupportsIdentity())
		{
			createTriggeredSequence(cw, objRes.getTableName());
		}

		if (adapter.isRequiresCommitAfterTableCreation())
		{
			cw.commit();
		}
		if (!objRes.isPrimitive() && !objRes.isArray())
		{
			// create an entry in the IS_A table
			createClassRelation(objRes.getRepresentedClass(), cw);
		}
		objRes.ensureContainedTablesExist(cw);
	}

	/**
	 * Add all relations in the IS_A table. This function should only be called
	 * once per class, since it is only called from createTable which is only
	 * called if the table doesn't already exist. It will insert relations for
	 * superclasses and all interfaces.
	 * 
	 * @throws SQLException
	 */
	private void createClassRelation(Class<?> subClass, ConnectionWrapper cw) throws SQLException
	{
		String subClassName = ObjectTools.getSystemicName(subClass);
		// insert relation for the superclass
		Class<?> superClass = subClass.getSuperclass();
		if (superClass != null)
		{
			addClassRelationConditionally(subClassName, ObjectTools.getSystemicName(superClass), cw);
		}
		// insert relations for all interfaces
		Class<?>[] interfaces = subClass.getInterfaces();
		for (Class<?> infc : interfaces)
		{
			addClassRelationConditionally(subClassName, ObjectTools.getSystemicName(infc), cw);
			// recurse into super-interfaces
			createClassRelation(infc, cw);
		}
	}

	/**
	 * Checks if a given IS-A relationship exists, adds it if not.
	 * 
	 * 
	 * @param subClass
	 *            the name of the subclass, implementing class, or subinterface.
	 * @param superClass
	 *            the name of the superclass, superinterface, or the implemented
	 *            interface.
	 * @param cw
	 *            the connection wrapper to execute the commands.
	 * 
	 * @throws SQLException
	 */
	private void addClassRelationConditionally(String subClass, String superClass, ConnectionWrapper cw)
			throws SQLException
	{
		PreparedStatement query = cw.prepareStatement("SELECT COUNT(*) FROM " + Defaults.IS_A_TABLENAME
				+ " WHERE SUBCLASS = ? AND SUPERCLASS = ?");
		query.setString(1, subClass);
		query.setString(2, superClass);
		Tools.logFine(query);
		try
		{
			ResultSet rs = query.executeQuery();
			if (rs.next())
			{
				int count = rs.getInt(1);
				if (count == 0)
				{
					addClassRelation(subClass, superClass, cw);
				}
			}
		}
		finally
		{
			query.close();
		}
	}

	public void ensureColumnExists(String tableName, String columnName, Class<?> paramType, ConnectionWrapper cw)
			throws SQLException
	{
		if (createSchema)
		{
			if (!columnExists(tableName, columnName, cw))
			{
				String tableType = adapter.getColumnType(paramType, null).trim();
				createColumn(tableName, columnName, tableType, cw);
			}
		}
	}

	/**
	 * Check that the table(s) to hold the object represented by objRes exists,
	 * create it if not.
	 * 
	 * @param objRes
	 * @param cw
	 * @throws SQLException
	 * @throws SchemaPermissionException
	 */
	public void ensureTableExists(ConcreteObjectRepresentation objRes, ConnectionWrapper cw) throws SQLException
	{
		if (createSchema)
		{
			try
			{
				if (!tableExists(objRes, cw))
				{
					// if not, create it
					createTable(objRes, cw);
				}
			}
			catch (SchemaPermissionException e)
			{
				throw new SQLException(e);
			}
		}
	}

	/**
	 * Make sure a table exists.
	 * 
	 * @param c
	 * @throws SQLException
	 * @throws SchemaPermissionException
	 */
	public void ensureTableExists(Class<?> c, ConnectionWrapper cw) throws SQLException
	{
		if (createSchema)
		{
			if (c != null)
			{
				ObjectStack oStack = new ObjectStack(this.adapter, c, null);
				for (int x = 0; x < oStack.getSize(); x++)
				{
					ensureTableExists((ConcreteObjectRepresentation)oStack.getRepresentation(x), cw);
				}
			}
		}
	}

	/**
	 * Load a list of all classes stored in the database from the IS_A table.
	 * 
	 * @param cw
	 * @throws SQLException
	 * @throws
	 */
	private List<Class<?>> populateClassList(ConnectionWrapper cw) throws SQLException
	{
		List<Class<?>> res = new ArrayList<Class<?>>();
		try
		{
			// find all sub-classes
			PreparedStatement ps = cw.prepareStatement("SELECT DISTINCT(SUBCLASS) FROM " + Defaults.IS_A_TABLENAME);
			Tools.logFine(ps);
			if (ps.execute())
			{
				ResultSet rs = ps.getResultSet();
				while (rs.next())
				{
					String name = rs.getString(1);
					Class<?> c = ObjectTools.lookUpClass(name);
					res.add(c);
					if (!existingClasses.contains(c))
					{
						existingClasses.add(c);
					}
				}
			}
			ps.close();
			// find all super-classes
			ps = cw.prepareStatement("SELECT DISTINCT(SUPERCLASS) FROM " + Defaults.IS_A_TABLENAME);
			Tools.logFine(ps);
			if (ps.execute())
			{
				ResultSet rs = ps.getResultSet();
				while (rs.next())
				{
					String name = rs.getString(1);
					Class<?> c = ObjectTools.lookUpClass(name);
					if (!res.contains(c))
					{
						res.add(c);
					}
					if (!existingClasses.contains(c))
					{
						existingClasses.add(c);
					}
				}
			}
			ps.close();
		}
		catch (ClassNotFoundException cnfe)
		{
			throw new SQLException(cnfe);
		}
		return res;
	}

	/**
	 * Get all direct subclasses of the given class.
	 * 
	 * @param superclass
	 * @return
	 * @throws SQLException
	 */
	private ArrayList<Class<?>> getSubClasses(Class<?> superclass, ConnectionWrapper cw) throws SQLException
	{
		ArrayList<Class<?>> res = new ArrayList<Class<?>>();
		try
		{
			StringBuilder query = new StringBuilder("SELECT SUBCLASS FROM ");
			query.append(Defaults.IS_A_TABLENAME);
			query.append(" WHERE SUPERCLASS = ?");
			PreparedStatement ps = cw.prepareStatement(query.toString());
			ps.setString(1, ObjectTools.getSystemicName(superclass));
			Tools.logFine(ps);
			if (ps.execute())
			{
				ResultSet rs = ps.getResultSet();
				while (rs.next())
				{
					String name = rs.getString(1);
					Class<?> c = ObjectTools.lookUpClass(name);
					res.add(c);
				}
				rs.close();
			}
			ps.close();
		}
		catch (ClassNotFoundException cnfe)
		{
			throw new SQLException(cnfe);
		}

		return res;
	}

	/**
	 * Get a list of all superclasses and interfaces implemented directly by
	 * this class, according to the database.
	 * 
	 * @param subClass
	 * @param cw
	 * @return
	 * @throws SQLException
	 */
	private List<Class<?>> getSuperClasses(Class<?> subClass, ConnectionWrapper cw) throws SQLException
	{
		List<Class<?>> res = new ArrayList<Class<?>>();
		try
		{
			StringBuilder query = new StringBuilder("SELECT SUPERCLASS FROM ");
			query.append(Defaults.IS_A_TABLENAME);
			query.append(" WHERE SUBCLASS = ?");
			PreparedStatement ps = cw.prepareStatement(query.toString());
			ps.setString(1, ObjectTools.getSystemicName(subClass));
			Tools.logFine(ps);
			if (ps.execute())
			{
				ResultSet rs = ps.getResultSet();
				while (rs.next())
				{
					String name = rs.getString(1);
					Class<?> c = ObjectTools.lookUpClass(name);
					res.add(c);
				}
				rs.close();
			}
			ps.close();
		}
		catch (ClassNotFoundException cnfe)
		{
			throw new SQLException(cnfe);
		}

		return res;

	}

	/**
	 * Helper method for {@link #dropTableForClass(Class, ConnectionWrapper)}.
	 * 
	 * @param c
	 * @param cw
	 * @throws SQLException
	 */
	private void dropTableHelper(Class<?> c, ConnectionWrapper cw, List<Class<?>> classList) throws SQLException
	{
		// get the name of the table
		String tableName = NameGenerator.getTableName(c, adapter);
		// check if the table has been deleted
		if (existingClasses.contains(c))
		{
			existingClasses.remove(c);

			// remove all protection entries
			adapter.getPersist().getProtectionManager().unprotectObjects(cw, tableName);
			// delete all instances of the class
			adapter.getPersist().deleteObjects(cw, c, new All());
			// drop table of subclasses
			ArrayList<Class<?>> subClasses = this.getSubClasses(c, cw);
			for (Class<?> subClass : subClasses)
			{
				dropTableHelper(subClass, cw, classList);
			}
			//delete meta-info
			deleteIsATableEntries(c, cw);
			removeTypeInfo(tableName, cw);
			
			// drop the table
			conditionalDelete(tableName, cw);
			if (!adapter.isSupportsIdentity())
			{
				// this adapter relies on sequences, so drop the corresponding
				// sequence
				String sequenceName = Tools.getSequenceName(tableName, adapter);
				String dropGeneratorQuery = "DROP GENERATOR " + sequenceName;

				PreparedStatement ps = cw.prepareStatement(dropGeneratorQuery);
				Tools.logFine(ps);
				ps.execute();
				ps.close();
			}
			// find all classes that reference c, delete them.
			ArrayList<Class<?>> referencingClasses = getReferencingClasses(c, classList);
			for (Class<?> ref : referencingClasses)
			{
				dropTableHelper(ref, cw, classList);
			}
		}
	}


	private void conditionalDelete(String tableName, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder query = new StringBuilder("DROP TABLE ");
		if (this.adapter.isSupportsExistsKeyword())
		{
			query.append("IF EXISTS ");
		}
		query.append(tableName);
		PreparedStatement ps = null;
		try
		{
			ps = cw.prepareStatement(query.toString());
			Tools.logFine(ps);
			ps.execute();
		}
		catch (SQLException e)
		{
			// check the SQLSTATE
			if (e.getSQLState() != null)
			{
				if (e.getSQLState().toUpperCase().equals("42Y55"))
				{
				}
				if (e.getSQLState().toUpperCase().equals("S0002"))
				{
					// HSQLDB table not found
				}
				else
				{
					// this is not an error indicating the non-existence of the
					// table we tried to delete, so re-throw it
					throw e;
				}
			}
			// handle badly implemented drivers that do not have correct
			// SQLstate.
			else if (e.getMessage().toLowerCase().contains("no such table"))
			{
			}
			else
			{
				throw e;
			}
		}
		finally
		{
			if (ps != null)
			{
				ps.close();
			}
		}
	}

	/**
	 * Get all classes that have direct references to class c.
	 * 
	 * @param c
	 *            the class to look for references to.
	 * @return
	 */
	private ArrayList<Class<?>> getReferencingClasses(Class<?> c, List<Class<?>> classList)
	{
		// we don't care about self-reference
		classList.remove(c);
		ArrayList<Class<?>> res = new ArrayList<Class<?>>();
		// iterate over all the candidate referencing classes
		for (Class<?> cand : classList)
		{

			Method[] methods = cand.getDeclaredMethods();
			for (Method m : methods)
			{
				if (ObjectTools.isValidMethod(m))
				{
					Class<?> propertyType = m.getReturnType();

					if (m.isAnnotationPresent(AsClob.class) && m.getReturnType().equals(char[].class)
							&& adapter.isSupportsClob())
					{
						propertyType = Clob.class;
					}
					else if (m.isAnnotationPresent(AsBlob.class) && m.getReturnType().equals(byte[].class)
							&& adapter.isSupportsBlob())
					{
						propertyType = Blob.class;
					}
					// if the property type of cand equals c, add cand to the
					// list of classes that reference c.
					if (propertyType.equals(c))
					{
						res.add(cand);
					}
				}
			}
		}
		return res;
	}

	/**
	 * Delete all entries from the IS_A table that references class c.
	 * 
	 * @param c
	 * @param cw
	 * @throws SQLException
	 */
	private void deleteIsATableEntries(Class<?> c, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder query = new StringBuilder("DELETE FROM ");
		query.append(Defaults.IS_A_TABLENAME);
		query.append(" WHERE SUPERCLASS = ? OR SUBCLASS = ?");
		PreparedStatement ps = cw.prepareStatement(query.toString());
		String className = ObjectTools.getSystemicName(c);
		ps.setString(1, className);
		ps.setString(2, className);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
	}

	private void deleteClassRelation(String superClass, String subClass, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder query = new StringBuilder("DELETE FROM ");
		query.append(Defaults.IS_A_TABLENAME);
		query.append(" WHERE SUPERCLASS = ? AND SUBCLASS = ?");
		PreparedStatement ps = cw.prepareStatement(query.toString());
		ps.setString(1, superClass);
		ps.setString(2, subClass);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
	}

	/**
	 * Drops all tables that comprise the given class.
	 * 
	 * Tables are dropped whether they are empty or not. All subclasses are also
	 * dropped. If c is an interface, all implementing classes and their
	 * subclasses will be dropped. All classes that reference this class (or any
	 * of its subclasses) will also be dropped.
	 * 
	 * @param c
	 * @param cw
	 * @throws SQLException
	 */
	public synchronized void dropTableForClass(Class<?> c, ConnectionWrapper cw) throws SQLException
	{
		// only drop tables if we can create tables.
		if (this.createSchema)
		{
			List<Class<?>> classList = populateClassList(cw);
			dropTableHelper(c, cw, classList);
		}
	}

	/**
	 * Check for changed column names, update table.
	 * 
	 * @param klass
	 * @param cw
	 * @throws SQLException
	 * @throws SchemaPermissionException
	 * @throws IntegrityException
	 */
	public void renameColumns(Class<?> klass, ConnectionWrapper cw) throws SQLException, SchemaPermissionException,
			IntegrityException
	{
		// only drop tables if we can create tables.
		if (this.createSchema)
		{
			// get list of old property-type pairs
			ObjectRepresentation objRes = new ConcreteObjectRepresentation(adapter, klass, null);
			String tableName = objRes.getTableName();
			// find the list of name type pairs for the corresponding
			// database table
			Map<String, String> valueTypeMap = getDatabaseColumns(tableName, cw);
			List<String> oldNames = new ArrayList<String>();
			List<String> oldTypes = new ArrayList<String>();
			for (Entry<String, String> en : valueTypeMap.entrySet())
			{
				if (!en.getKey().toUpperCase().startsWith("C__"))
				{
					oldNames.add(en.getKey());
					oldTypes.add(en.getValue());
				}
			}
			// get list of new property-type pairs
			List<String> nuNames = new ArrayList<String>();
			List<String> nuTypes = new ArrayList<String>();
			for (int propIndex = 0; propIndex < objRes.getPropertyCount(); propIndex++)
			{
				nuNames.add(objRes.getPropertyName(propIndex));
				nuTypes.add(adapter.getColumnType(objRes.getReturnType(propIndex), objRes.getMutator(propIndex)));
			}
			for (int oldIndex = 0; oldIndex < oldNames.size(); oldIndex++)
			{
				String oldName = oldNames.get(oldIndex);
				if (nuNames.contains(oldName))
				{
					int nuIndex = nuNames.indexOf(oldName);
					// name is unchanged, remove
					oldNames.remove(oldIndex);
					oldTypes.remove(oldIndex);
					nuNames.remove(nuIndex);
					nuTypes.remove(nuIndex);
					oldIndex--;
				}
			}
			// sanity check
			if (oldNames.size() != nuNames.size())
			{
				throw new IntegrityException("The number of properties has changed, use updateSchema(...) instead.");
			}
			// iterate over the old names, find matching new name
			for (int oldIndex = 0; oldIndex < oldNames.size(); oldIndex++)
			{
				String oldType = oldTypes.get(oldIndex);
				int nuIndex = nuTypes.indexOf(oldType);
				// make sure there is only one column of this type in both old
				// and new version of object
				if (oldTypes.indexOf(oldType) != oldTypes.lastIndexOf(oldType)
						|| nuIndex != nuTypes.lastIndexOf(oldType))
				{
					throw new IntegrityException("More than one changed property of type " + oldType + " in table "
							+ tableName);
				}
				// execute schema update
				renameColumn(tableName, oldNames.get(oldIndex), nuNames.get(nuIndex), cw);
			}
		}
		else
		{
			throw new SchemaPermissionException("We do not have permission to change the database schema.");
		}
	}

	private void renameColumn(String tableName, String oldName, String nuName, ConnectionWrapper cw)
			throws SQLException
	{
		// check that the old column exists
		if (adapter.tableNamesAreLowerCase())
		{
			tableName = tableName.toLowerCase();
		}
		Map<String, String> columns = this.getDatabaseColumns(tableName, cw);
		if (columns.containsKey(oldName))
		{
			if (adapter.canRenameColumn())
			{
				String statement = adapter.getRenameColumnStatement();
				statement = statement.replace(Defaults.TABLENAME_PLACEHOLDER, tableName);
				statement = statement.replace(Defaults.OLD_COLUMN_NAME_PLACEHOLDER, oldName);
				statement = statement.replace(Defaults.NEW_COLUMN_NAME_PLACEHOLDER, nuName);
				statement = statement.replace(Defaults.NEW_COLUMN_DESCRIPTION_PLACEHOLDER, columns.get(oldName));
				PreparedStatement ps = cw.prepareStatement(statement);
				Tools.logFine(ps);
				ps.executeUpdate();
				ps.close();
			}
			else
			{
				// can't rename column

				// get the old colums
				Map<String, String> oldCols = this.getDatabaseColumns(tableName, cw);
				Map<String, String> nuCols = new HashMap<String, String>();
				nuCols.putAll(oldCols);
				// rename the column in nuCols
				String type = oldCols.get(oldName);
				nuCols.remove(oldName);
				// create new table, temporary name
				String temporaryName = "T__" + tableName;
				if (adapter.tableNamesAreLowerCase())
				{
					temporaryName = temporaryName.toLowerCase();
				}
				while (temporaryName.length() > adapter.getMaximumNameLength())
				{
					temporaryName = temporaryName.substring(0, temporaryName.length() - 1);
				}
				StringBuilder stmt = new StringBuilder("CREATE TABLE ");
				stmt.append(temporaryName);
				stmt.append(" (");
				List<String> sameColums = new ArrayList<String>();
				for (Entry<String, String> en : nuCols.entrySet())
				{
					String key = en.getKey();
					sameColums.add(key);
					stmt.append(key);
					stmt.append(" ");
					stmt.append(en.getValue());
					if (key.equalsIgnoreCase(Defaults.ID_COL))
					{
						stmt.append(" PRIMARY KEY");
					}
					stmt.append(",");
				}
				stmt.append(nuName);
				stmt.append(" ");
				stmt.append(type);
				stmt.append(")");
				PreparedStatement ps = cw.prepareStatement(stmt.toString());
				Tools.logFine(ps);
				ps.execute();
				ps.close();

				// copy from old table
				stmt = new StringBuilder("INSERT INTO ");
				stmt.append(temporaryName);
				stmt.append(" (");
				for (String colName : sameColums)
				{
					stmt.append(colName);
					stmt.append(",");
				}
				stmt.append(nuName);
				stmt.append(") SELECT ");
				;
				for (String colName : sameColums)
				{
					stmt.append(colName);
					stmt.append(",");
				}
				stmt.append(oldName);
				stmt.append(" FROM ");
				stmt.append(tableName);
				ps = cw.prepareStatement(stmt.toString());
				Tools.logFine(ps);
				ps.execute();
				ps.close();

				// drop old table
				conditionalDelete(tableName, cw);
				// rename new table
				this.setTableName(temporaryName, tableName, cw);
			}
		}
	}

	/**
	 * @param klass
	 * @param cw
	 * @throws SQLException
	 * @throws SchemaPermissionException
	 * @throws ClassNotFoundException 
	 */
	public void updateTableForClass(Class<?> klass, ConnectionWrapper cw) throws SQLException,
			SchemaPermissionException, ClassNotFoundException
	{
		// only update tables if we are allowed to
		if (this.createSchema)
		{
			// check that this class is not an array or primitive
			if (!klass.isArray() && !ObjectTools.isDatabasePrimitive(klass))
			{

				// get info on the superclass
				Class<?> superClass = klass.getSuperclass();
				String superClassName = ObjectTools.getSystemicName(superClass);

				List<Class<?>> superClasses = getSuperClasses(klass, cw);
				ObjectStack nuObjectStack = new ObjectStack(adapter, klass);

				// check if the real superclass is correctly indicated by the
				// database
				if (!superClasses.contains(superClass))
				{
					// klass has been moved, it now has a new superclass.
					SubclassMover sm = new SubclassMover(adapter);
					ObjectStack oldObjectStack = getObjectStackFromDatabase(klass, cw);
					sm.move(oldObjectStack, nuObjectStack, nuObjectStack.getActualRepresentation(), cw);
					// update the C_IS_A table to reflect new superclass
					String klassName = ObjectTools.getSystemicName(klass);
					addClassRelation(klassName, superClassName, cw);
					String oldSuperClass = oldObjectStack.getRepresentation(oldObjectStack.getLevel(klass) - 1)
							.getSystemicName();
					deleteClassRelation(oldSuperClass, klassName, cw);
				}

				// make sure each entry in superClasses is still in the current
				// list
				// get a list of implemented interfaces
				Class<?>[] interfaces = klass.getInterfaces();
				for (Class<?> dbSuperClass : superClasses)
				{
					if (dbSuperClass.isInterface())
					{
						// check if the class is one of the existing interfaces
						// as indicated by the database
						boolean exists = false;
						for (Class<?> iface : interfaces)
						{
							if (iface.equals(dbSuperClass))
							{
								exists = true;
								break;
							}
						}
						if (!exists)
						{
							// the superclass is no longer a superclass
							// delete the entry
							deleteClassRelation(ObjectTools.getSystemicName(dbSuperClass),
									ObjectTools.getSystemicName(klass), cw);
						}
					}
				}

				// check that all implemented interfaces are correctly indicated
				// by the database
				for (Class<?> iface : interfaces)
				{
					if (!superClasses.contains(iface))
					{
						// update the C_IS_A table to reflect new interface
						addClassRelation(ObjectTools.getSystemicName(klass), ObjectTools.getSystemicName(iface), cw);
					}
				}

				// Check if any property has been moved up or down
				for (int level = nuObjectStack.getSize() - 1; level > 0; level--)
				{
					String tablename = nuObjectStack.getRepresentation(level).getTableName();
					// find the list of name type pairs for the corresponding
					// database table
					Map<String, String> valueTypeMap = getDatabaseColumns(tablename, cw);

					for (Entry<String, String> en : valueTypeMap.entrySet())
					{
						String colName = en.getKey();
						int correctLevel = nuObjectStack.getRepresentationLevel(colName);
						if (correctLevel != level && correctLevel >= 0)
						{
							moveField(nuObjectStack, colName, level, correctLevel, cw);
						}
					}

				}

				ObjectRepresentation toRep = nuObjectStack.getActualRepresentation();
				ObjectRepresentation fromRep = new DatabaseObjectRepresentation(adapter, klass, cw);
				
				try
				{
					ChangeDescription change = fromRep.getDifference(toRep);
					if(change != null)
					{
						TableManager tm = adapter.getPersist().getTableManager();
						if(change.isDeletion())
						{
							tm.dropColumn(toRep.getTableName(), change.getFromName(),cw);
						}
						else if(change.isCreation())
						{
							String columnType = adapter.getColumnType(change.getToClass(), null).trim();
							tm.createColumn(toRep.getTableName(), change.getToName(),columnType, cw);
						}
						else if(change.isNameChange())
						{
							tm.renameColumn(toRep.getTableName(), change.getFromName(), change.getToName(), cw);
						}
						else if(change.isTypeChange())
						{
							if(CompabilityCalculator.calculate(change.getFromClass(), change.getToClass()))
							{
								//TODO: Handle this
							}
							else
							{
								//no conversion, drop and recreate.
								tm.dropColumn(toRep.getTableName(), change.getFromName(),cw);
								String columnType = adapter.getColumnType(change.getToClass(), null).trim();
								tm.createColumn(toRep.getTableName(), change.getToName(),columnType, cw);
							}
						}
					}
				}
				catch (MetadataException e)
				{
					throw new SQLException(e);
				}
			}
		}
		else
		{
			throw new SchemaPermissionException("We do not have permission to change the database schema.");
		}
	}

	/**
	 * Move the field colName from the table at level fromLevel to the table at
	 * level toLevel
	 * 
	 * @param nuObjectStack
	 * @param colName
	 * @param fromLevel
	 * @param toLevel
	 * @param cw
	 * @throws SQLException
	 */
	private void moveField(ObjectStack nuObjectStack, String colName, int fromLevel, int toLevel, ConnectionWrapper cw)
			throws SQLException
	{
		// generate aliases
		UniqueIdTree idTree = new UniqueIdTree(new UniqueIdGenerator());
		idTree.nameStack(nuObjectStack);

		String fromTable = nuObjectStack.getRepresentation(fromLevel).getTableName();
		String fromTableAs = nuObjectStack.getRepresentation(fromLevel).getAsName();
		String toTable = nuObjectStack.getRepresentation(toLevel).getTableName();
		String toTableAs = nuObjectStack.getRepresentation(toLevel).getAsName();

		// create a new field of the right type in toTable
		ensureColumnExists(toTable, colName, nuObjectStack.getRepresentation(toLevel).getReturnType(colName), cw);

		IdStatementGenerator idGen = new IdStatementGenerator(adapter, nuObjectStack, true);
		int minLevel = Math.min(fromLevel, toLevel);
		String idStatement = idGen.generate(minLevel);
		StringBuilder sb = new StringBuilder("UPDATE ");
		sb.append(toTable);
		sb.append(" AS ");
		sb.append(toTableAs);
		sb.append(" SET ");
		sb.append(toTableAs);
		sb.append(".");
		sb.append(colName);
		sb.append("= ( SELECT ");
		sb.append(fromTableAs);
		sb.append(".");
		sb.append(colName);
		sb.append(" FROM ");
		sb.append(idGen.generateAsStatement(new String[] { toTable }));
		sb.append(" WHERE ");
		sb.append(idStatement);
		sb.append(")");
		PreparedStatement ps = cw.prepareStatement(sb.toString());
		int index = 0;
		for (RelationDescriptor o : idGen.getRelationDescriptors())
		{
			if (o.isRequiresvalue())
			{
				index++;
				Tools.setParameter(ps, o.getValue().getClass(), index, o.getValue());
			}
		}
		Tools.logFine(ps);
		ps.executeUpdate();
		ps.close();

		// drop the field in fromTable
		try
		{
			dropColumn(fromTable, colName, cw);
		}
		catch (ClassNotFoundException e)
		{
			throw new SQLException(e);
		}
	}

	/**
	 * Get the object stack, according to the database.
	 * 
	 * @param klass
	 * @return
	 * @throws SQLException
	 */
	private ObjectStack getObjectStackFromDatabase(Class<?> klass, ConnectionWrapper cw) throws SQLException
	{
		List<Class<?>> list = new ArrayList<Class<?>>();
		list.add(klass);
		while (!klass.equals(Object.class))
		{
			// find the actual superclass of klass
			List<Class<?>> supers = getSuperClasses(klass, cw);
			for (Class<?> s : supers)
			{
				if (!s.isInterface())
				{
					klass = s;
					list.add(0, klass);
					break;
				}
			}
		}
		ObjectStack res = new ObjectStack(adapter, list);
		return res;
	}

	/**
	 * Check if any columns have been removed or changed type.
	 * 
	 * @param newRep
	 * @param valueTypeMap
	 * @param cw
	 * @throws SQLException
	 */
	private void removeObsoleteColumns(ObjectRepresentation newRep, Map<String, String> valueTypeMap,
			ConnectionWrapper cw) throws SQLException
	{
		String tableName = newRep.getTableName();
		// check if any column in the database is obsolete
		for (Entry<String, String> valueType : valueTypeMap.entrySet())
		{
			String colName = valueType.getKey();
			if (!colName.equals(Defaults.ID_COL) && !colName.equals(Defaults.DUMMY_COL_NAME)
					&& !colName.equals(Defaults.REAL_CLASS_COL) && !colName.equals(Defaults.REAL_ID_COL))
			{
				try
				{
					if (!newRep.hasProperty(colName))
					{
						// column no longer exists
						// get parent class with property, if it exists
						ObjectStack stack = new ObjectStack(adapter, newRep.getClass());
						ObjectRepresentation parentWithProperty = stack.getRepresentation(colName);
						// if so, copy all values to parent class
						if (parentWithProperty != null)
						{
							// TODO: Implement this
						}
						else
						{
							// move contents to subclasses
							copyDown(newRep, newRep, colName, cw);
						}
						// drop column
						dropColumn(tableName, colName, cw);
					}
					else
					{
						// check that the type is the same
						Class<?> newColumnClass = newRep.getReturnType(colName);
						String newColumnType = adapter.getColumnType(newColumnClass, null);
						if (!adapter.columnTypesEqual(newColumnType, valueType.getValue()))
						{
							// column has changed type
							if (columnIsCompatible(tableName,colName,newColumnClass,cw))
							{
								// TODO: Change the column type 

							}
							else
							{
								// drop column
								dropColumn(tableName, colName, cw);
								// create new column with the new type
								createColumn(tableName, colName, newColumnType, cw);
							}
							//update the meta-information
							changeTypeInfo(tableName, colName, newColumnClass, cw);
						}
						else if (newColumnType.equalsIgnoreCase(adapter.getReferenceType(newRep.getReturnType(colName))))
						{
							// compare contents to HAS_A table.
							boolean wasReference = checkIfColumnWasReference(tableName, colName, cw);
							// column has not changed type, but column
							// is reference type
							if (newRep.isPrimitive(colName))
							{
								// the column points to a Long
								// make sure there are no objects
								// referenced by this column
								if (wasReference)
								{
									// drop referenced objects
									dropUprotectedReferences(tableName, colName, cw);
								}
							}
							else
							{
								// the column now points to reference
								// type

								if (wasReference)
								{
									// Column was reference, is now
									// reference:
									// Check that all referenced objects
									// are compatible
									dropIncompatibleReferences(tableName, colName, newRep.getReturnType(colName), cw);
									// TODO: Update reference type of
									// remaining entries
								}
								else
								{
									// Column was long, is now
									// reference:
									// Drop column, add new column, all
									// references are null
									dropColumn(tableName, colName, cw);
									createColumn(tableName, colName, newColumnType, cw);
								}
							}

						}
					}
				}
				catch (ClassNotFoundException e)
				{
					throw new SQLException(e);
				}
			}
		}
	}

	/**
	 * @param columnType
	 * @param value
	 * @return
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	private boolean columnIsCompatible(String tableName, String columnName, Class<?>toClass,ConnectionWrapper cw) throws SQLException, ClassNotFoundException
	{
		//get the corresponding java types
		Class<?> fromClass = lookupType(tableName,columnName,cw);
		return CompabilityCalculator.calculate(fromClass,toClass);
	}

	/**
	 * @param tableName
	 * @param columnName
	 * @return
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	private Class<?> lookupType(String tableName, String columnName,ConnectionWrapper cw) throws SQLException, ClassNotFoundException
	{

		StringBuilder statement = new StringBuilder("SELECT ");
		statement.append("COLUMN_CLASS FROM ");
		statement.append(Defaults.TYPE_TABLENAME);
		statement.append(" WHERE OWNER_TABLE=? AND COLUMN_NAME=?");

		PreparedStatement ps = cw.prepareStatement(statement.toString());
		ps.setString(1, tableName);
		ps.setString(2, columnName);
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		if (rs.next())
		{
			String value = rs.getString(1);
			return ObjectTools.lookUpClass(value);			
		}
		else
		{
			throw new SQLException("Could not find type for "+tableName+"."+columnName);
		}
	}

	/**
	 * Move a named column from one class to its subclasses.
	 * 
	 * @param owners
	 *            the tables that used to contain the column
	 * @param objRes
	 *            the direct parent of the class to copy from.
	 * @param colName
	 *            the column to copy.
	 * @param cw
	 * @throws SQLException
	 */
	private void copyDown(ObjectRepresentation from, ObjectRepresentation to, String colName, ConnectionWrapper cw)
			throws SQLException
	{
		// get all direct subclasses
		InheritanceModel iModel = new InheritanceModel(to.getRepresentedClass(), cw);
		List<Class<?>> subs = iModel.getDirectSubclasses();
		for (Class<?> s : subs)
		{
			ObjectRepresentation tmpRes = new ConcreteObjectRepresentation(adapter, s, null);
			if (tmpRes.hasProperty(colName))
			{
				// find the list of name type pairs for the corresponding
				// database table
				Map<String, String> valueTypeMap = getDatabaseColumns(tmpRes.getTableName(), cw);
				// check if any columns have been removed
				removeObsoleteColumns(tmpRes, valueTypeMap, cw);
				// add all columns to the table
				addNewColumns(tmpRes, valueTypeMap, cw);

				StatementPrototypeGenerator statementGenerator = new StatementPrototypeGenerator(adapter);
				StatementPrototype sp = statementGenerator.generate(s, true);
				String fromName = statementGenerator.getTypeStack().getRepresentation(from.getRepresentedClass())
						.getTableName();
				String toName = statementGenerator.getTypeStack().getActualRepresentation().getTableName();
				String prestatement = "UPDATE " + toName + " SET " + toName + "." + colName + "=" + fromName + "."
						+ colName + " ";
				PreparedStatement ps = sp.toPreparedStatement(cw, prestatement);
				Tools.logFine(ps);
				ps.executeUpdate();
			}
			else
			{
				// check if the column exists in a subclass of the subclass
				copyDown(from, tmpRes, colName, cw);
			}
		}

	}

	/**
	 * Add new columns to the class
	 * 
	 * @param cw
	 * @param valueTypeMap
	 * @throws SQLException
	 */
	private void addNewColumns(ObjectRepresentation objRes, Map<String, String> valueTypeMap, ConnectionWrapper cw)
			throws SQLException
	{
		String tableName = objRes.getTableName();
		for (int property = 0; property < objRes.getPropertyCount(); property++)
		{
			String propertyName = objRes.getPropertyName(property);
			if (!valueTypeMap.containsKey(propertyName))
			{
				// create a new column
				String tableType = adapter.getColumnType(objRes.getReturnType(property), null).trim();
				createColumn(tableName, propertyName, tableType, cw);
			}
		}
	}

	/**
	 * Check that all objects referenced from tableName via colName are of type
	 * returnType or a subtype of return type.
	 * 
	 * @param tableName
	 * @param colName
	 * @param returnType
	 * @param cw
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private void dropIncompatibleReferences(String tableName, String colName, Class<?> returnType, ConnectionWrapper cw)
			throws SQLException, ClassNotFoundException
	{

		// Get affected entries from HAS_A table
		StringBuilder statement = new StringBuilder("SELECT ");
		statement.append("OWNER_ID,");
		statement.append("PROPERTY_TABLE,");
		statement.append("PROPERTY_ID,");
		statement.append("PROPERTY_CLASS");
		statement.append(" FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE OWNER_TABLE=? AND (");
		statement.append(Defaults.RELATION_NAME_COL);
		statement.append("=? OR ");
		statement.append(Defaults.RELATION_NAME_COL);
		statement.append(" IS NULL)");

		PreparedStatement ps = cw.prepareStatement(statement.toString());
		ps.setString(1, tableName);
		ps.setString(2, colName);
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		ProtectionManager pm = adapter.getPersist().getProtectionManager();
		while (rs.next())
		{
			// get data on one instance
			Long ownerId = rs.getLong(1);
			String propertyTable = rs.getString(2);
			Long propertyId = rs.getLong(3);
			String propertyClassName = rs.getString(4);
			// check compability
			if (!ObjectTools.isA(ObjectTools.lookUpClass(propertyClassName), returnType))
			{
				// remove protection
				pm.unprotectObjectInternal(tableName, ownerId, propertyTable, propertyId, cw);
				// if entity is unprotected,
				if (!pm.isProtected(propertyClassName, propertyId, cw))
				{
					// then delete the entity
					adapter.getPersist().deleteObject(ObjectTools.lookUpClass(propertyClassName), propertyId, cw);
				}
			}
		}
	}

	/**
	 * @param tableName
	 * @param colName
	 * @param cw
	 * @return false if the named table has a named column that contains longs,
	 *         true if it contains references.
	 * @throws SQLException
	 */
	private boolean checkIfColumnWasReference(String tableName, String colName, ConnectionWrapper cw)
			throws SQLException
	{
		StringBuilder getAllEntriesQuery = new StringBuilder("SELECT COUNT(*) FROM ");
		getAllEntriesQuery.append(Defaults.HAS_A_TABLENAME);
		getAllEntriesQuery.append(" WHERE OWNER_TABLE = ?");
		getAllEntriesQuery.append(" AND ");
		getAllEntriesQuery.append(Defaults.RELATION_NAME_COL);
		getAllEntriesQuery.append("= ?");
		PreparedStatement ps = cw.prepareStatement(getAllEntriesQuery.toString());
		ps.setString(1, tableName);
		ps.setString(2, colName);
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		rs.next();
		long count = rs.getLong(1);
		ps.close();
		rs.close();

		return (count > 0);
	}

	/**
	 * @param className
	 * @param superClassName
	 * @param cw
	 * @throws SQLException
	 */
	private void addClassRelation(String className, String superClassName, ConnectionWrapper cw) throws SQLException
	{
		PreparedStatement ps = cw.prepareStatement("INSERT INTO " + Defaults.IS_A_TABLENAME
				+ " (SUBCLASS,SUPERCLASS) values (?,?)");
		ps.setString(1, className);
		ps.setString(2, superClassName);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
	}

	/**
	 * Create a new column in the given table with the given name and type.
	 * 
	 * @param tableName
	 * @param column
	 * @param tableType
	 * @param cw
	 * @throws SQLException
	 */
	private void createColumn(String tableName, String column, String tableType, ConnectionWrapper cw)
			throws SQLException
	{
		PreparedStatement ps = cw.prepareStatement("ALTER TABLE " + tableName + " ADD " + column + " " + tableType);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
	}

	/**
	 * Drop a named column.
	 * 
	 * @param tableName
	 * @param column
	 * @param cw
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public void dropColumn(String tableName, String column, ConnectionWrapper cw) throws SQLException,
			ClassNotFoundException
	{
		dropUprotectedReferences(tableName, column, cw);
		StringBuilder sb = new StringBuilder("ALTER TABLE ");
		sb.append(tableName);
		sb.append(" DROP ");
		sb.append(column);
		PreparedStatement ps = cw.prepareStatement(sb.toString());
		Tools.logFine(ps);
		ps.execute();
		ps.close();
		
		//remove the column from the type table
		sb = new StringBuilder("DELETE FROM ");
		sb.append(Defaults.TYPE_TABLENAME);
		sb.append(" WHERE OWNER_TABLE = ? AND COLUMN_NAME = ?");
		ps = cw.prepareStatement(sb.toString());
		ps.setString(1,tableName);
		ps.setString(2, column);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
		
	}

	private void dropUprotectedReferences(String tableName, String column, ConnectionWrapper cw) throws SQLException,
			ClassNotFoundException
	{
		// Get affected entries from HAS_A table
		StringBuilder statement = new StringBuilder("SELECT ");
		statement.append("OWNER_ID,");
		statement.append("PROPERTY_TABLE,");
		statement.append("PROPERTY_ID,");
		statement.append("PROPERTY_CLASS");
		statement.append(" FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE OWNER_TABLE=? AND (");
		statement.append(Defaults.RELATION_NAME_COL);
		statement.append("=? OR ");
		statement.append(Defaults.RELATION_NAME_COL);
		statement.append(" IS NULL)");

		PreparedStatement ps = cw.prepareStatement(statement.toString());
		ps.setString(1, tableName);
		ps.setString(2, column);
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		ProtectionManager pm = adapter.getPersist().getProtectionManager();
		while (rs.next())
		{
			// get data on one instance
			Long ownerId = rs.getLong(1);
			String propertyTable = rs.getString(2);
			Long propertyId = rs.getLong(3);
			String propertyClassName = rs.getString(4);
			if (propertyClassName != null)
			{
				// remove protection
				pm.unprotectObjectInternal(tableName, ownerId, propertyTable, propertyId, cw);
				// if entity is unprotected,
				if (!pm.isProtected(propertyClassName, propertyId, cw))
				{
					// then delete the entity
					Class<?> c = ObjectTools.lookUpClass(propertyClassName);
					adapter.getPersist().deleteObject(c, propertyId, cw);
				}
			}
			else
			{
				// we're dealing with an array, delete it especially
				adapter.getPersist().deleteObject(propertyTable, propertyId, cw);
			}
		}
		rs.close();
		ps.close();
	}

	/**
	 * Get a map of tablename->type for the given table.
	 * 
	 * @param tableName
	 * @param cw
	 * @return
	 * @throws SQLException
	 */
	public Map<String, String> getDatabaseColumns(String tableName, ConnectionWrapper cw) throws SQLException
	{
		//TODO: Use metadata table instead
		Map<String, String> res = new HashMap<String, String>();

		Connection c = cw.getConnection();

		DatabaseMetaData metaData = c.getMetaData();
		ResultSet rs = metaData.getColumns(c.getCatalog(), null, tableName, null);
		while (rs.next())
		{
			res.put(rs.getString(4), rs.getString(6));
		}
		return res;
	}

	/**
	 * This method will fail if we do not have schema modification enabled.
	 * 
	 * @param oldClass
	 * @param newClass
	 * @throws SQLException
	 * @throws SchemaPermissionException
	 *             if we are not allowed to modify the schema.
	 */
	public void setTableName(Class<?> oldClass, Class<?> newClass, ConnectionWrapper cw) throws SQLException,
			SchemaPermissionException
	{

		if (this.createSchema)
		{
			String oldTableName = NameGenerator.getTableName(oldClass, adapter);
			String newTableName = NameGenerator.getTableName(newClass, adapter);
			String oldClassName = ObjectTools.getSystemicName(oldClass);
			String newClassName = ObjectTools.getSystemicName(newClass);

			String newArrayMemberTable = NameGenerator.getArrayMemberTableName(newClass, adapter);

			// check if the table names has changed
			if (!oldTableName.equals(newTableName))
			{
				// update the cache
				adapter.getPersist().getCache().purge(oldTableName);

				// alter the table name
				setTableName(oldTableName, newTableName, cw);

				// change the array tables
				updateAllRelations(Defaults.ARRAY_TABLENAME, Defaults.COMPONENT_TABLE_COL, oldTableName, newTableName,
						cw);
				setTableName(NameGenerator.getArrayMemberTableName(oldClass, adapter), newArrayMemberTable, cw);

				// Update ownership relations
				updateAllRelations(Defaults.HAS_A_TABLENAME, "OWNER_TABLE", oldTableName, newTableName, cw);
				updateAllRelations(Defaults.HAS_A_TABLENAME, "PROPERTY_TABLE", oldTableName, newTableName, cw);
				
				//update type info table
				updateAllRelations(Defaults.TYPE_TABLENAME,"OWNER_TABLE",oldTableName,newTableName,cw);
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
						updateSuperClass(NameGenerator.getTableName(oldSuperClass, adapter), oldClassName,
								newClassName, cw);
					}
				}
				// update array tables
				updateAllRelations(Defaults.ARRAY_TABLENAME, Defaults.COMPONENT_CLASS_COL, oldClassName, newClassName,
						cw);
				if (tableExists(newArrayMemberTable, cw))
				{
					updateAllRelations(newArrayMemberTable, Defaults.COMPONENT_CLASS_COL, oldClassName, newClassName,
							cw);
					// update all C_ARRAY_MEMBER_<newName> entries
					updateAllRelations(newArrayMemberTable, Defaults.COMPONENT_CLASS_COL, oldClassName, newClassName,
							cw);
				}

				// Update ownership relations
				updateAllRelations(Defaults.HAS_A_TABLENAME, "PROPERTY_CLASS", oldClassName, newClassName, cw);
			}
		}
		else
		{
			throw new SchemaPermissionException("We do not have permission to change the database schema.");
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
		if (this.tableExists(superClassTableName, cw))
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

	/**
	 * Change the name of a table from oldName to newName. No other changes will
	 * be effected.
	 * 
	 * @param oldName
	 * @param newName
	 * @throws SQLException
	 */
	private void setTableName(String oldName, String newName, ConnectionWrapper cw) throws SQLException
	{
		if (tableExists(oldName, cw))
		{
			PreparedStatement ps = cw.prepareStatement("ALTER TABLE " + oldName + " RENAME TO " + newName);
			Tools.logFine(ps);
			ps.execute();
			ps.close();
		}
	}
	
	public void changeTypeInfo(String tableName, String propertyName, Class<?>returnType,ConnectionWrapper cw) throws SQLException
	{
		removeTypeInfo(tableName,propertyName,cw);
		addTypeInfo(tableName,propertyName,returnType,cw);
	}

	/**
	 * Add information about the return type of a given property of a given table.
	 * @param tableName
	 * @param propertyName
	 * @param returnType
	 * @throws SQLException 
	 */
	public void addTypeInfo(String tableName, String propertyName, Class<?> returnType,ConnectionWrapper cw) throws SQLException
	{
		StringBuilder stmt = new StringBuilder("INSERT INTO ");
		stmt.append(Defaults.TYPE_TABLENAME);
		stmt.append(" (OWNER_TABLE ,COLUMN_NAME ,COLUMN_CLASS) VALUES (?,?,?)");
		PreparedStatement ps = cw.prepareStatement(stmt.toString());
		ps.setString(1, tableName);
		ps.setString(2, propertyName);
		ps.setString(3, ObjectTools.getSystemicName(returnType));
		Tools.logFine(ps);
		ps.execute();
		ps.close();
	}
	/**
	 * Remove information about the return type of a given property of a given table.
	 * @param tableName
	 * @param propertyName
	 * @throws SQLException 
	 */
	public void removeTypeInfo(String tableName, String propertyName,ConnectionWrapper cw) throws SQLException
	{
		StringBuilder stmt = new StringBuilder("DELETE FROM ");
		stmt.append(Defaults.TYPE_TABLENAME);
		stmt.append(" WHERE OWNER_TABLE = ? AND COLUMN_NAME = ?");
		PreparedStatement ps = cw.prepareStatement(stmt.toString());
		ps.setString(1, tableName);
		ps.setString(2, propertyName);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
		
	}
	
	/**
	 * @param tableName
	 * @param cw
	 * @throws SQLException 
	 */
	private void removeTypeInfo(String tableName, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder stmt = new StringBuilder("DELETE FROM ");
		stmt.append(Defaults.TYPE_TABLENAME);
		stmt.append(" WHERE OWNER_TABLE = ? ");
		PreparedStatement ps = cw.prepareStatement(stmt.toString());
		ps.setString(1, tableName);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
	}
	
}
