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
import java.util.Set;

import org.conserve.adapter.AdapterBase;
import org.conserve.annotations.AsBlob;
import org.conserve.annotations.AsClob;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.connection.DataConnectionPool;
import org.conserve.exceptions.SchemaPermissionException;
import org.conserve.select.All;
import org.conserve.tools.generators.IdStatementGenerator;
import org.conserve.tools.generators.RelationDescriptor;
import org.conserve.tools.generators.SubclassMover;
import org.conserve.tools.metadata.FieldChangeDescription;
import org.conserve.tools.metadata.InheritanceChangeCalculator;
import org.conserve.tools.metadata.InheritanceChangeDescription;
import org.conserve.tools.metadata.ConcreteObjectRepresentation;
import org.conserve.tools.metadata.DatabaseObjectRepresentation;
import org.conserve.tools.metadata.MetadataException;
import org.conserve.tools.metadata.ObjectRepresentation;
import org.conserve.tools.metadata.ObjectStack;
import org.conserve.tools.metadata.ObjectStack.Node;
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
	private int schemaTypeVersion = 2;
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
				// schema version table does not exist, create it
				createTable(Defaults.SCHEMA_VERSION_TABLENAME, new String[] { "VERSION" }, new String[] { adapter.getIntegerTypeKeyword() }, cw);

				// insert the current version
				String commandString = "INSERT INTO  " + Defaults.SCHEMA_VERSION_TABLENAME + "  (VERSION ) values (?)";
				PreparedStatement ps = cw.prepareStatement(commandString);
				ps.setInt(1, schemaTypeVersion);
				Tools.logFine(ps);
				ps.execute();
				ps.close();
				existingSchema = schemaTypeVersion;
			}
			if (existingSchema < schemaTypeVersion)
			{
				upgradeSchema(existingSchema, cw);
			}
			else if (existingSchema > schemaTypeVersion)
			{
				throw new SQLException("Database schema is version " + existingSchema + " but Conserve is version " + schemaTypeVersion);
			}

			// check that we have a table to store indices in before we try to
			// create indices
			if (!tableExists(Defaults.INDEX_TABLENAME, cw))
			{
				if (!this.createSchema)
				{
					throw new SchemaPermissionException(Defaults.INDEX_TABLENAME + " does not exist, but can't create it.");
				}
				createTable(Defaults.INDEX_TABLENAME, new String[] { "TABLE_NAME", "COLUMN_NAME", "INDEX_NAME" },
						new String[] { adapter.getVarCharKeyword(), adapter.getVarCharKeyword(), adapter.getVarCharKeyword() }, cw);
			}
			if (!tableExists(Defaults.IS_A_TABLENAME, cw))
			{
				if (!this.createSchema)
				{
					throw new SchemaPermissionException(Defaults.IS_A_TABLENAME + " does not exist, but can't create it.");
				}
				createTable(Defaults.IS_A_TABLENAME, new String[] { "SUPERCLASS", "SUBCLASS" },
						new String[] { adapter.getVarCharIndexed(), adapter.getVarCharIndexed() }, cw);
				// create an index on the superclass name, since this is the
				// one we will be searching for most frequently
				createIndex(Defaults.IS_A_TABLENAME, new String[] { "SUPERCLASS" + adapter.getKeyLength() },
						Defaults.IS_A_TABLENAME + "_SUPERCLASS_INDEX", cw);
			}
			if (!tableExists(Defaults.HAS_A_TABLENAME, cw))
			{
				if (!this.createSchema)
				{
					throw new SchemaPermissionException(Defaults.HAS_A_TABLENAME + " does not exist, but can't create it.");
				}

				createTable(Defaults.HAS_A_TABLENAME,
						new String[] { "OWNER_TABLE", "OWNER_ID", Defaults.RELATION_NAME_COL, "PROPERTY_TABLE", "PROPERTY_ID", "PROPERTY_CLASS" },
						new String[] { adapter.getVarCharIndexed(), adapter.getLongTypeKeyword(), adapter.getVarCharKeyword(),
								adapter.getVarCharIndexed(), adapter.getLongTypeKeyword(), adapter.getVarCharIndexed() },
						cw);

				// create an index on the tablename/id combinations, since
				// this is the one we will be searching for most frequently
				createIndex(Defaults.HAS_A_TABLENAME, new String[] { "OWNER_TABLE" + adapter.getKeyLength(), "OWNER_ID" },
						Defaults.HAS_A_TABLENAME + "_OWNER_INDEX", cw);
				createIndex(Defaults.HAS_A_TABLENAME, new String[] { "PROPERTY_TABLE" + adapter.getKeyLength(), "PROPERTY_ID" },
						Defaults.HAS_A_TABLENAME + "_PROPERTY_INDEX", cw);

			}
			if (!tableExists(Defaults.ARRAY_TABLENAME, cw))
			{
				if (!this.createSchema)
				{
					throw new SchemaPermissionException(Defaults.ARRAY_TABLENAME + " does not exist, but can't create it.");
				}
				if (adapter.isSupportsIdentity())
				{
					// create the table with an identity column
					createTable(Defaults.ARRAY_TABLENAME,
							new String[] { Defaults.ID_COL, Defaults.COMPONENT_TABLE_COL, Defaults.COMPONENT_CLASS_COL },
							new String[] { adapter.getIdentity() + " PRIMARY KEY", adapter.getVarCharIndexed(), adapter.getVarCharIndexed() }, cw);
				}
				else
				{
					// the adapter does not support identity
					// check if we can use a trigger
					if (adapter.isSupportsTriggers())
					{
						// create the table as usual
						createTable(Defaults.ARRAY_TABLENAME,
								new String[] { Defaults.ID_COL, Defaults.COMPONENT_TABLE_COL, Defaults.COMPONENT_CLASS_COL }, new String[] {
										adapter.getLongTypeKeyword() + " PRIMARY KEY", adapter.getVarCharIndexed(), adapter.getVarCharIndexed() },
								cw);
						// create the trigger sequence
						createTriggeredSequence(cw, Defaults.ARRAY_TABLENAME);

					}
					else
					{
						throw new RuntimeException("Database engines without both autoincrements and triggers are not supported at this time.");
					}
				}
				// create an index on the id, as this is the one we
				// will be searching for most frequently
				createIndex(Defaults.ARRAY_TABLENAME, new String[] { Defaults.ID_COL }, Defaults.ARRAY_TABLENAME + "_INDEX", cw);
			}

			if (!tableExists(Defaults.ARRAY_MEMBER_TABLE_NAME_ARRAY, cw))
			{
				if (!this.createSchema)
				{
					throw new SchemaPermissionException(Defaults.ARRAY_MEMBER_TABLE_NAME_ARRAY + " does not exist, but can't create it.");
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
						throw new RuntimeException("Database engines without both autoincrements and triggers are not supported at this time.");
					}

				}
			}

			if (!tableExists(Defaults.TYPE_TABLENAME, cw))
			{
				if (!this.createSchema)
				{
					throw new SchemaPermissionException(Defaults.TYPE_TABLENAME + " does not exist, but can't create it.");
				}
				// create the type table
				createTable(Defaults.TYPE_TABLENAME, new String[] { "OWNER_TABLE", "COLUMN_NAME", "COLUMN_CLASS" },
						new String[] { adapter.getVarCharIndexed(), adapter.getVarCharIndexed(), adapter.getVarCharIndexed() }, cw);

			}

			if (!tableExists(Defaults.TABLE_NAME_TABLENAME, cw))
			{
				if (!this.createSchema)
				{
					throw new SchemaPermissionException(Defaults.TABLE_NAME_TABLENAME + " does not exist, but can't create it.");
				}
				// create the table to store associations between class names
				// and table names
				createTable(Defaults.TABLE_NAME_TABLENAME, new String[] { "CLASS", "TABLENAME" },
						new String[] { adapter.getVarCharIndexed(), adapter.getVarCharIndexed() }, cw);
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
		if (existingSchema < 2)
		{
			throw new RuntimeException("This version of Conserve can not load databases with version less than 2.");
		}
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
		String toExectue = "CREATE TRIGGER " + triggerName + " FOR " + tableName + " ACTIVE BEFORE INSERT POSITION 0\n" + "AS\n" + "BEGIN \n"
				+ "if (NEW." + Defaults.ID_COL + " is NULL) then NEW." + Defaults.ID_COL + " = GEN_ID(" + sequenceName + ", 1);\n"
				+ "RDB$SET_CONTEXT('USER_SESSION', 'LAST__INSERT__ID', new." + Defaults.ID_COL + ");\n" + "END";
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
			columnName = columnName.toLowerCase();
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
		rs.close();
		return res;
	}

	private void createTable(ConcreteObjectRepresentation objRes, ConnectionWrapper cw) throws SQLException, SchemaPermissionException
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

		if (!adapter.isSupportsIdentity() && objRes.getRepresentedClass().equals(Object.class))
		{
			createTriggeredSequence(cw, objRes.getTableName());
		}

		if (adapter.isRequiresCommitAfterTableCreation())
		{
			cw.commit();
		}
		if (!objRes.isPrimitive() && !objRes.isArray())
		{
			// create entries in the IS_A table
			createClassRelations(objRes.getRepresentedClass(), cw);
		}
		objRes.ensureContainedTablesExist(cw);

		if (!objRes.isPrimitive())
		{
			// store an association between the class name and the table name
			setTableNameForClass(objRes.getSystemicName(), objRes.getTableName(), cw);
		}
		createIndicesForTable(objRes, cw);
	}

	private void createIndicesForTable(ObjectRepresentation objRes, ConnectionWrapper cw) throws SQLException
	{
		// get the set of index names
		Set<String> indexNames = objRes.getIndexNames();
		for (String indexName : indexNames)
		{
			// get the list of all fields indexed by the named index
			List<String> indexedFields = objRes.getFieldNamesInIndex(indexName);
			String[] fieldArray = indexedFields.toArray(new String[0]);
			// TODO: check index key length
			for (int x = 0; x < fieldArray.length; x++)
			{
				if (objRes.getReturnType(fieldArray[x]).equals(String.class))
				{
					fieldArray[x] += adapter.getKeyLength();
				}
			}
			// create the index
			createIndex(objRes.getTableName(), fieldArray, indexName, cw);
		}
	}

	/**
	 * Add all relations in the IS_A table. This function should only be called
	 * once per class, since it is only called from createTable which is only
	 * called if the table doesn't already exist. It will insert relations for
	 * superclasses and all interfaces.
	 * 
	 * @throws SQLException
	 */
	private void createClassRelations(Class<?> subClass, ConnectionWrapper cw) throws SQLException
	{
		String subClassName = NameGenerator.getSystemicName(subClass);
		// insert relation for the superclass
		Class<?> superClass = subClass.getSuperclass();
		if (superClass != null)
		{
			addClassRelation(subClassName, NameGenerator.getSystemicName(superClass), cw);
			// recurse into super-superclass
			createClassRelations(superClass, cw);
		}
		// insert relations for all interfaces
		Class<?>[] interfaces = subClass.getInterfaces();
		for (Class<?> infc : interfaces)
		{
			addClassRelation(subClassName, NameGenerator.getSystemicName(infc), cw);
			// recurse into super-interfaces
			createClassRelations(infc, cw);
		}
	}

	public void ensureColumnExists(String tableName, String columnName, Class<?> paramType, ConnectionWrapper cw) throws SQLException
	{
		if (createSchema)
		{
			if (!columnExists(tableName, columnName, cw))
			{
				createColumn(tableName, columnName, paramType, cw);
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
				for (ObjectRepresentation rep : oStack.getAllRepresentations())
				{
					ensureTableExists((ConcreteObjectRepresentation) rep, cw);
				}
			}
		}
	}

	/**
	 * Load a list of all classes stored in the database from the IS_A table.
	 * 
	 * @param cw
	 * @throws SQLException
	 * 			@throws
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
					Class<?> c = ObjectTools.lookUpClass(name, adapter);
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
					Class<?> c = ObjectTools.lookUpClass(name, adapter);
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
	private List<Class<?>> getSubClasses(Class<?> superclass, ConnectionWrapper cw) throws SQLException
	{
		List<Class<?>> res = new ArrayList<Class<?>>();
		try
		{
			StringBuilder query = new StringBuilder("SELECT SUBCLASS FROM ");
			query.append(Defaults.IS_A_TABLENAME);
			query.append(" WHERE SUPERCLASS = ?");
			PreparedStatement ps = cw.prepareStatement(query.toString());
			ps.setString(1, NameGenerator.getSystemicName(superclass));
			Tools.logFine(ps);
			if (ps.execute())
			{
				ResultSet rs = ps.getResultSet();
				while (rs.next())
				{
					String name = rs.getString(1);
					Class<?> c = ObjectTools.lookUpClass(name, adapter);
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
	 * Get the name of all direct subclasses of the given class.
	 * 
	 * @param superclass
	 * @return
	 * @throws SQLException
	 */
	private List<String> getSubClassNames(Class<?> superclass, ConnectionWrapper cw) throws SQLException
	{
		List<String> res = new ArrayList<String>();
		StringBuilder query = new StringBuilder("SELECT SUBCLASS FROM ");
		query.append(Defaults.IS_A_TABLENAME);
		query.append(" WHERE SUPERCLASS = ?");
		PreparedStatement ps = cw.prepareStatement(query.toString());
		ps.setString(1, NameGenerator.getSystemicName(superclass));
		Tools.logFine(ps);
		if (ps.execute())
		{
			ResultSet rs = ps.getResultSet();
			while (rs.next())
			{
				String name = rs.getString(1);
				res.add(name);
			}
			rs.close();
		}
		ps.close();

		return res;
	}

	/**
	 * Helper method for {@link #dropTableForClass(Class, ConnectionWrapper)}.
	 * 
	 * 
	 * @param c
	 * @param cw
	 * @param classList
	 *            the list of all classes known to the system
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
			List<Class<?>> subClasses = this.getSubClasses(c, cw);
			for (Class<?> subClass : subClasses)
			{
				dropTableHelper(subClass, cw, classList);
			}
			// delete meta-info
			deleteIsATableEntries(c, cw);
			removeTypeInfo(tableName, cw);
			removeTableNameForClass(NameGenerator.getSystemicName(c), tableName, cw);

			//drop indices from table
			dropAllIndicesForTable(tableName, cw);
			// drop the table
			conditionalDelete(tableName, cw);
			if (!adapter.isSupportsIdentity() && c.equals(Object.class))
			{
				// this adapter relies on sequences, so drop the corresponding
				// sequence
				String sequenceName = Tools.getSequenceName(tableName, adapter);
				StringBuilder dropGeneratorQuery = new StringBuilder("DROP GENERATOR ");
				dropGeneratorQuery.append(sequenceName);

				PreparedStatement ps = cw.prepareStatement(dropGeneratorQuery.toString());
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
				else if (e.getSQLState().toUpperCase().equals("S0002"))
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
	 * Drop all indices that reference the named table.
	 * 
	 * @param tableName
	 * @param cw
	 * @throws SQLException
	 */
	private void dropAllIndicesForTable(String tableName, ConnectionWrapper cw) throws SQLException
	{
		// first, get a list of indices
		StringBuilder queryString = new StringBuilder("SELECT INDEX_NAME FROM ");
		queryString.append(Defaults.INDEX_TABLENAME);
		queryString.append(" WHERE TABLE_NAME = ?");
		PreparedStatement ps = cw.prepareStatement(queryString.toString());
		ps.setString(1, tableName);
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		List<String> indices = new ArrayList<String>();
		while (rs.next())
		{
			String indexName = rs.getString(1);
			if(!indices.contains(indexName))
			{
				indices.add(indexName);
			}
		}
		ps.close();

		// delete all the indices
		for (String index : indices)
		{
			dropIndex(tableName, index, cw);
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

					if (m.isAnnotationPresent(AsClob.class) && m.getReturnType().equals(char[].class) && adapter.isSupportsClob())
					{
						propertyType = Clob.class;
					}
					else if (m.isAnnotationPresent(AsBlob.class) && m.getReturnType().equals(byte[].class) && adapter.isSupportsBlob())
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
		String className = NameGenerator.getSystemicName(c);
		ps.setString(1, className);
		ps.setString(2, className);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
	}


	/**
	 * Delete the superclass-subclass relation for a pair of classes. Also
	 * update protection entries and references that are no longer satisfied.
	 * 
	 * @param superClass
	 *            the old superclass
	 * @param subClass
	 *            the new class
	 * @param cw
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private void deleteClassRelation(Class<?> superClass, Class<?> subClass, ConnectionWrapper cw) throws SQLException, ClassNotFoundException
	{
		String superClassName = NameGenerator.getSystemicName(superClass);
		String subClassName = NameGenerator.getSystemicName(subClass);
		// remove the relation from C__IS_A table
		StringBuilder query = new StringBuilder("DELETE FROM ");
		query.append(Defaults.IS_A_TABLENAME);
		query.append(" WHERE SUPERCLASS = ? AND SUBCLASS = ?");
		PreparedStatement ps = cw.prepareStatement(query.toString());
		ps.setString(1, superClassName);
		ps.setString(2, subClassName);
		Tools.logFine(ps);
		ps.execute();
		ps.close();

		deleteProtectionEntriesFromClassRelation(superClass, subClass, cw);
	}

	private void deleteProtectionEntriesFromClassRelation(Class<?> superClass, Class<?> subClass, ConnectionWrapper cw)
			throws SQLException, ClassNotFoundException
	{
		String superClassName = NameGenerator.getSystemicName(superClass);
		String subClassName = NameGenerator.getSystemicName(subClass);

		// find all classes that reference superClass or one of its
		// superclasses that are not common to the new superclass of
		// subClass. If any instance points to an object that is really
		// subClass, delete the protection entry. If the referenced object
		// is unprotected, delete that too.

		List<Class<?>> noLongerSupported = ObjectTools.getAllLegalReferenceTypes(superClass);
		List<Class<?>> stillSupported = ObjectTools.getAllLegalReferenceTypes(subClass);
		// remove all in noLongerSupported that are in stillSupported
		for (int x = 0; x < noLongerSupported.size(); x++)
		{
			if (stillSupported.contains(noLongerSupported.get(x)))
			{
				noLongerSupported.remove(x);
				x--;
			}
		}

		// get the protection manager
		ProtectionManager pm = adapter.getPersist().getProtectionManager();

		// find all rows in C__TYPE_TABLE where COLUMN_CLASS is
		// the old superclass.
		StringBuilder query = new StringBuilder("SELECT OWNER_TABLE,COLUMN_NAME FROM ");
		query.append(Defaults.TYPE_TABLENAME);
		query.append(" WHERE COLUMN_CLASS = ?");
		PreparedStatement stmt = cw.prepareStatement(query.toString());
		stmt.setString(1, superClassName);
		Tools.logFine(stmt);
		ResultSet tmpRes = stmt.executeQuery();
		while (tmpRes.next())
		{
			String ownerTable = tmpRes.getString(1);
			String relationName = tmpRes.getString(2);
			// find all entries in C__HAS_A (protection entries) where owner
			// and relation name is from the search results, and property is
			// the new subclass
			query = new StringBuilder("SELECT OWNER_ID,PROPERTY_TABLE, PROPERTY_ID FROM ");
			query.append(Defaults.HAS_A_TABLENAME);
			query.append(" WHERE OWNER_TABLE=? AND RELATION_NAME=? AND PROPERTY_CLASS=?");
			PreparedStatement innerStmt = cw.prepareStatement(query.toString());
			innerStmt.setString(1, ownerTable);
			innerStmt.setString(2, relationName);
			innerStmt.setString(3, subClassName);
			Tools.logFine(innerStmt);
			ResultSet innerRes = innerStmt.executeQuery();
			while (innerRes.next())
			{
				// remove the reference
				setReferenceTo(ownerTable, innerRes.getLong(1), relationName, null, cw);
				// remove protection entry
				pm.unprotectObjectInternal(ownerTable, innerRes.getLong(1), innerRes.getString(2), innerRes.getLong(3), cw);
				// if item is unprotected, remove it
				if (!pm.isProtected(innerRes.getString(2), innerRes.getLong(3), cw))
				{
					Class<?> lookUpClass = ObjectTools.lookUpClass(getClassForTableName(innerRes.getString(2), cw), adapter);
					adapter.getPersist().deleteObject(lookUpClass, innerRes.getLong(3), cw);
				}
			}
			innerStmt.close();

		}
		stmt.close();

		// recursively do the same for all direct subclasses of subClass.
		List<Class<?>> subs = getSubClasses(subClass, cw);
		for (Class<?> sub : subs)
		{
			deleteProtectionEntriesFromClassRelation(superClass, sub, cw);
		}
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
	 * Change the type of a named column. Also changes associated metadata.
	 * 
	 * @param tableName
	 *            the name of the column to change the type for.
	 * @param column
	 *            the column to change the type of.
	 * @param oldClass
	 *            the old type of the database column.
	 * @param nuClass
	 *            the type to change the column into.
	 * @param cw
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private void changeColumnType(String tableName, String column, Class<?> oldClass, Class<?> nuClass, ConnectionWrapper cw)
			throws SQLException, ClassNotFoundException
	{
		String oldType = adapter.getColumnType(oldClass, null);
		String nuType = adapter.getColumnType(nuClass, null);
		// only do conversion if it is necessary
		if (!oldType.equals(nuType))
		{
			if (adapter.canChangeColumnType())
			{
				StringBuilder sb = new StringBuilder("ALTER TABLE ");
				sb.append(tableName);
				sb.append(" ");
				sb.append(adapter.getColumnModificationKeyword());
				sb.append(" COLUMN ");
				sb.append(column);
				sb.append(" ");
				sb.append(adapter.getColumnModificationTypeKeyword());
				sb.append(" ");
				sb.append(nuType);
				PreparedStatement ps = cw.prepareStatement(sb.toString());
				Tools.logFine(ps);
				ps.execute();
				ps.close();
			}
			else
			{
				// we can't change column type
				// do a 4-step workaround
				// 1. rename the old column to a temporary name
				String nuName = "C__TEMP_NAME_" + column;
				renameColumn(tableName, column, nuName, cw);
				// 2. create a new column with the desired properties
				createColumn(tableName, column, nuClass, cw);
				// 3. copy all values from the old to the new column
				copyValues(tableName, nuName, column, cw);
				// 4. drop the old column
				dropColumn(tableName, nuName, cw);
			}
		}

		// store the new column metadata
		changeTypeInfo(tableName, column, nuClass, cw);
	}

	/**
	 * Copy all values from one column to another in the same table. Casting
	 * and/or conversion depends on the underlying database.
	 * 
	 * @param tableName
	 *            the table to copy values in.
	 * @param fromColumn
	 *            the column to copy from.
	 * @param toColumn
	 *            the column to copy to.
	 * @param cw
	 * @throws SQLException
	 */
	private void copyValues(String tableName, String fromColumn, String toColumn, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder sb = new StringBuilder("UPDATE ");
		sb.append(tableName);
		sb.append(" SET ");
		sb.append(toColumn);
		sb.append("=");
		sb.append(fromColumn);
		PreparedStatement ps = cw.prepareStatement(sb.toString());
		Tools.logFine(ps);
		ps.executeUpdate();
		ps.close();
	}

	/**
	 * Rename a column.
	 * 
	 * @param tableName
	 * @param oldName
	 * @param nuName
	 * @param cw
	 * @throws SQLException
	 */
	private void renameColumn(String tableName, String oldName, String nuName, ConnectionWrapper cw) throws SQLException
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

			// Change name in C__TYPE_TABLE
			StringBuilder stmt = new StringBuilder("UPDATE ");
			stmt.append(Defaults.TYPE_TABLENAME);
			stmt.append(" SET COLUMN_NAME = ? WHERE OWNER_TABLE  = ? AND COLUMN_NAME = ?");
			PreparedStatement ps = cw.prepareStatement(stmt.toString());
			ps.setString(1, nuName);
			ps.setString(2, tableName);
			ps.setString(3, oldName);
			Tools.logFine(ps);
			ps.execute();
			ps.close();

			// Change name in C__HAS_A
			stmt = new StringBuilder("UPDATE ");
			stmt.append(Defaults.HAS_A_TABLENAME);
			stmt.append(" SET RELATION_NAME = ? WHERE OWNER_TABLE  = ? AND RELATION_NAME = ?");
			ps = cw.prepareStatement(stmt.toString());
			ps.setString(1, nuName);
			ps.setString(2, tableName);
			ps.setString(3, oldName);
			Tools.logFine(ps);
			ps.execute();
			ps.close();
		}
	}

	/**
	 * @param klass
	 * @param cw
	 * @throws SQLException
	 * @throws SchemaPermissionException
	 * @throws ClassNotFoundException
	 */
	public void updateTableForClass(Class<?> klass, ConnectionWrapper cw) throws SQLException, SchemaPermissionException, ClassNotFoundException
	{
		// only update tables if we are allowed to
		if (this.createSchema)
		{
			// check that this class is not an array or primitive
			if (!klass.isArray() && !ObjectTools.isDatabasePrimitive(klass))
			{

				// read the old objectstack from the database
				ObjectStack oldObjectStack = new ObjectStack(cw, adapter, klass);
				// create the new objectstack from introspection
				ObjectStack nuObjectStack = new ObjectStack(adapter, klass);

				InheritanceChangeCalculator calc = new InheritanceChangeCalculator(oldObjectStack, nuObjectStack);
				InheritanceChangeDescription inheritanceChanges = calc.calculateDescription();
				// create the new links
				for (List<Node> nuClasses : inheritanceChanges.getAddedSuperClasses())
				{
					// add entries
					addEntries(nuClasses, cw);
				}
				// move moved fields
				List<FieldChangeDescription> movedFields = inheritanceChanges.getMovedFields();
				for (FieldChangeDescription movedField : movedFields)
				{
					// move fields
					moveFields(movedField, cw);
				}
				// delete entries in old tables
				for (List<Node> deletedClasses : inheritanceChanges.getRemovedSuperClasses())
				{
					// remove entries
					removeEntries(deletedClasses, cw);
				}

				// get info on direct subclasses from database
				List<String> subClasses = getSubClassNames(klass, cw);
				for (String subClass : subClasses)
				{
					// check if any subclass in the database has been removed
					try
					{
						ObjectTools.lookUpClass(subClass, adapter);
					}
					catch (ClassNotFoundException e)
					{
						// if so:
						// drop the table
						String tableName = getTableNameForClass(subClass, cw);

						// drop the table
						conditionalDelete(tableName, cw);
						dropAllIndicesForTable(tableName, cw);
						if (!adapter.isSupportsIdentity())
						{
							// this adapter relies on sequences, so drop the
							// corresponding
							// sequence
							String sequenceName = Tools.getSequenceName(tableName, adapter);
							String dropGeneratorQuery = "DROP GENERATOR " + sequenceName;

							PreparedStatement ps = cw.prepareStatement(dropGeneratorQuery);
							Tools.logFine(ps);
							ps.execute();
							ps.close();
						}
						// updating references not necessary, no class should
						// have reference to the removed class prior to removing
						// it

						// update subclass entries
						dropAllSubclassEntries(NameGenerator.getTableName(klass, adapter), subClass, cw);

						// update protection entries
						updateAllRelations(Defaults.HAS_A_TABLENAME, "PROPERTY_TABLE", tableName, NameGenerator.getTableName(klass, adapter), cw);
						updateAllRelations(Defaults.HAS_A_TABLENAME, "PROPERTY_CLASS", subClass, NameGenerator.getSystemicName(klass), cw);

						// update type table: only property class, as no
						// properties should be left in the subclass before
						// removing it
						updateAllRelations(Defaults.TYPE_TABLENAME, "COLUMN_CLASS", subClass, NameGenerator.getSystemicName(klass), cw);

					}
				}

				// only check for changed properties if the inheritance model
				// hasn't changed
				// it's not possible to change both at once
				if (!inheritanceChanges.inheritanceModelChanged())
				{
					//TODO: This should be handled already, can this code be removed?
					// Check if any property has been moved up or down
					for (int level = nuObjectStack.getSize() - 1; level > 0; level--)
					{
						String tablename = nuObjectStack.getRepresentation(level).getTableName();
						// find the list of name type pairs for the
						// corresponding database table
						Map<String, String> valueTypeMap = getDatabaseColumns(tablename, cw);

						for (Entry<String, String> en : valueTypeMap.entrySet())
						{
							String colName = en.getKey();
							Integer correctLevel = nuObjectStack.getRepresentationLevel(colName);
							if (correctLevel != null && correctLevel != level)
							{
								moveField(nuObjectStack, colName, level, correctLevel, cw);
							}
						}

					}

					// check if fields have changed
					ObjectRepresentation fromRep = new DatabaseObjectRepresentation(adapter, klass, cw);
					ObjectRepresentation toRep = nuObjectStack.getActualRepresentation();
					try
					{
						FieldChangeDescription change = fromRep.getFieldDifference(toRep);
						if (change != null)
						{
							if (change.isDeletion())
							{
								dropColumn(toRep.getTableName(), change.getFromName(), cw);
							}
							else if (change.isCreation())
							{
								createColumn(toRep.getTableName(), change.getToName(), change.getToClass(), cw);
							}
							else if (change.isNameChange())
							{
								renameColumn(toRep.getTableName(), change.getFromName(), change.getToName(), cw);
							}
							else if (change.isTypeChange())
							{
								if (CompabilityCalculator.calculate(change.getFromClass(), change.getToClass()))
								{
									// there is a conversion available
									// change the column type
									changeColumnType(toRep.getTableName(), change.getToName(), change.getFromClass(), change.getToClass(), cw);

									// Update object references and remove
									// incompatible entries
									updateReferences(toRep.getTableName(), change.getToName(), change.getFromClass(), change.getToClass(), cw);
								}
								else
								{
									// no conversion, drop and recreate.
									dropColumn(toRep.getTableName(), change.getFromName(), cw);
									createColumn(toRep.getTableName(), change.getToName(), change.getToClass(), cw);
								}
							}
							else if (change.isIndexChange())
							{
								// indexes have changed
								recreateIndices(toRep, cw);
							}
						}
					}
					catch (MetadataException e)
					{
						throw new SQLException(e);
					}
				}
			}
		}
		else
		{
			throw new SchemaPermissionException("We do not have permission to change the database schema.");
		}
	}

	/**
	 * Carry out the field movement description in the argument, moving all
	 * fields from one class to the other.
	 * 
	 * @param movedField
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private void moveFields(FieldChangeDescription movedField, ConnectionWrapper cw) throws SQLException, ClassNotFoundException
	{
		// create the new field
		if (!columnExists(movedField.getToTable(), movedField.getToName(), cw))
		{
			createColumn(movedField.getToTable(), movedField.getToName(), movedField.getToClass(), cw);
		}
		// copy all values
		if (adapter.isSupportsJoinInUpdate())
		{
			StringBuilder query = new StringBuilder();
			query.append("UPDATE ");
			query.append(movedField.getToTable());
			query.append(" SET ");
			// query.append(movedField.getToTable());
			// query.append(".");
			query.append(movedField.getToName());
			query.append(" = (SELECT ");
			query.append(movedField.getFromTable());
			query.append(".");
			query.append(movedField.getFromName());
			query.append(" FROM ");
			query.append(movedField.getFromTable());
			query.append(" WHERE ");
			query.append(movedField.getFromTable());
			query.append(".");
			query.append(Defaults.ID_COL);
			query.append(" = ");
			query.append(movedField.getToTable());
			query.append(".");
			query.append(Defaults.ID_COL);
			query.append(")");
			PreparedStatement ps = cw.prepareStatement(query.toString());
			Tools.logFine(ps);
			ps.executeUpdate();
		}
		else
		{
			// we'll have to join manually
			// underlying database does not support joins in UPDATE statements,
			// use alternate form
			StringBuilder query = new StringBuilder();
			query.append("UPDATE ");
			query.append(movedField.getToTable());
			query.append(" SET ");
			query.append(movedField.getToName());
			query.append(" = (SELECT ");
			query.append(movedField.getFromName());
			query.append(" FROM ");
			query.append(movedField.getFromTable());
			query.append(" WHERE ");
			query.append(Defaults.ID_COL);
			query.append(" = ");
			query.append(movedField.getToTable());
			query.append(".");
			query.append(Defaults.ID_COL);
			query.append(")");
			PreparedStatement ps = cw.prepareStatement(query.toString());
			Tools.logFine(ps);
			ps.executeUpdate();

		}
		
		//move protection entries
		//move table names
		StringBuilder sb = new StringBuilder("UPDATE ");
		sb.append(Defaults.HAS_A_TABLENAME);
		sb.append(" SET OWNER_TABLE = ? WHERE OWNER_TABLE = ? AND RELATION_NAME = ?");
		PreparedStatement ps = cw.prepareStatement(sb.toString());
		ps.setString(1, movedField.getToTable());
		ps.setString(2, movedField.getFromTable());
		ps.setString(3, movedField.getFromName());
		ps.executeUpdate();
		ps.close();
		
		//move field names
		sb = new StringBuilder("UPDATE ");
		sb.append(Defaults.HAS_A_TABLENAME);
		sb.append(" SET RELATION_NAME = ? WHERE OWNER_TABLE = ? AND RELATION_NAME = ?");
		ps = cw.prepareStatement(sb.toString());
		ps.setString(1, movedField.getToName());
		ps.setString(2, movedField.getToTable());
		ps.setString(3, movedField.getFromName());
		ps.executeUpdate();
		ps.close();

		// old class doesn't need this field anymore
		dropColumn(movedField.getFromTable(), movedField.getFromName(), cw);

	}

	/**
	 * Add the tables for any class that does not exist, then add entries for
	 * all entries in the first entry in the nuClasses list.
	 * 
	 * @param nuClasses
	 *            a list of new classes, the first entry is an existing class.
	 * @throws SchemaPermissionException
	 * @throws SQLException
	 */
	private void addEntries(List<Node> nuClasses, ConnectionWrapper cw) throws SQLException, SchemaPermissionException
	{
		// make sure all tables exists
		for (int tableIdx = 1; tableIdx < nuClasses.size(); tableIdx++)
		{
			Node n = nuClasses.get(tableIdx);
			ensureTableExists((ConcreteObjectRepresentation) n.getRepresentation(), cw);
		}

		// update the C_IS_A table to reflect new superclass
		for (int tableIdx = 0; tableIdx < nuClasses.size(); tableIdx++)
		{
			Node n = nuClasses.get(tableIdx);
			for (Node sup : n.getSupers())
			{
				addClassRelation(n.getRepresentation().getSystemicName(), sup.getRepresentation().getSystemicName(), cw);
			}
		}

		// get the id of all entries in the fist class
		String baseTable = nuClasses.get(0).getRepresentation().getTableName();
		StringBuilder query = new StringBuilder();
		query.append("SELECT ");
		query.append(Defaults.ID_COL);
		query.append(" FROM ");
		query.append(baseTable);
		PreparedStatement ps = cw.prepareStatement(query.toString());
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		while (rs.next())
		{
			long id = rs.getLong(1);
			for (int x = 0; x < nuClasses.size(); x++)
			{
				Node nuClass = nuClasses.get(x);
				if (x > 0)
				{
					// if we're above the base table,
					// add a new entry
					StringBuilder insert = new StringBuilder();
					insert.append("INSERT INTO ");
					insert.append(nuClass.getRepresentation().getTableName());
					insert.append("(");
					insert.append(Defaults.ID_COL);
					insert.append(",");
					insert.append(Defaults.REAL_CLASS_COL);
					insert.append(")values(?,?)");
					PreparedStatement pInsert = cw.prepareStatement(insert.toString());
					pInsert.setLong(1, id);
					pInsert.setString(2, nuClasses.get(x - 1).getRepresentation().getSystemicName());
					Tools.logFine(pInsert);
					pInsert.executeUpdate();
					pInsert.close();
				}
				// re-point all the relevant superclasses
				List<Node> supers = nuClass.getSupers();
				for (Node sup : supers)
				{
					if ((x >= nuClasses.size() - 1) || !sup.equals(nuClasses.get(x + 1)))
					{
						StringBuilder updateReal = new StringBuilder();
						updateReal.append("UPDATE ");
						updateReal.append(sup.getRepresentation().getTableName());
						updateReal.append(" SET ");
						updateReal.append(Defaults.REAL_CLASS_COL);
						updateReal.append(" = ? WHERE ");
						updateReal.append(Defaults.ID_COL);
						updateReal.append(" = ?");
						PreparedStatement pUpd = cw.prepareStatement(updateReal.toString());
						pUpd.setString(1, nuClass.getRepresentation().getSystemicName());
						pUpd.setLong(2, id);
						Tools.logFine(pUpd);
						pUpd.executeUpdate();
						pUpd.close();
					}
				}

			}
		}
		ps.close();

	}

	/**
	 * 
	 * @param deletedClasses
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private void removeEntries(List<Node> deletedClasses, ConnectionWrapper cw) throws ClassNotFoundException, SQLException
	{

		// get the id of all entries in the fist class
		String baseTable = deletedClasses.get(0).getRepresentation().getTableName();
		StringBuilder query = new StringBuilder();
		query.append("SELECT ");
		query.append(Defaults.ID_COL);
		query.append(" FROM ");
		query.append(baseTable);
		PreparedStatement ps = cw.prepareStatement(query.toString());
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		while (rs.next())
		{
			long id = rs.getLong(1);
			for (int x = 1; x < deletedClasses.size(); x++)
			{
				Node nuClass = deletedClasses.get(x);
				// remove the entries
				StringBuilder insert = new StringBuilder();
				insert.append("DELETE FROM ");
				insert.append(nuClass.getRepresentation().getTableName());
				insert.append(" WHERE ");
				insert.append(Defaults.ID_COL);
				insert.append(" = ?");
				PreparedStatement pInsert = cw.prepareStatement(insert.toString());
				pInsert.setLong(1, id);
				Tools.logFine(pInsert);
				pInsert.executeUpdate();
				pInsert.close();

			}
		}
		ps.close();

		if (deletedClasses.size() > 1)
		{
			// remove the immediate super-class IS_A relationship
			Node n = deletedClasses.get(0);
			Node sup = deletedClasses.get(1);
			deleteClassRelation(sup.getRepresentation().getRepresentedClass(), n.getRepresentation().getRepresentedClass(), cw);
		}

	}

	/**
	 * Recreate the indices by dropping all indices and re-creating them.
	 * 
	 * @param objRep
	 *            the description of the class to change
	 * @throws SQLException
	 */
	private void recreateIndices(ObjectRepresentation objRep, ConnectionWrapper cw) throws SQLException
	{
		dropAllIndicesForTable(objRep.getTableName(), cw);
		createIndicesForTable(objRep, cw);
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
	private void moveField(ObjectStack nuObjectStack, String colName, int fromLevel, int toLevel, ConnectionWrapper cw) throws SQLException
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

		if (adapter.isSupportsJoinInUpdate())
		{
			IdStatementGenerator idGen = new IdStatementGenerator(adapter, nuObjectStack, null, true);

			// copy the values
			StringBuilder sb = new StringBuilder();
			String idStatement = idGen.generate();
			sb.append("UPDATE ");
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
		}
		else
		{
			// underlying database does not support joins in UPDATE statements,
			// use alternate form
			//select all C__ID+values
			StringBuilder select = new StringBuilder("SELECT ");
			select.append(Defaults.ID_COL);
			select.append(",");
			select.append(colName);
			select.append(" FROM ");
			select.append(fromTable);
			PreparedStatement selectStatement = cw.prepareStatement(select.toString());
			ResultSet selected = selectStatement.executeQuery();
			//create the update statement
			StringBuilder update = new StringBuilder("UPDATE ");
			update.append(toTable);
			update.append(" SET ");
			update.append(colName);
			update.append(" = ? WHERE ");
			update.append(Defaults.ID_COL);
			update.append(" = ?");
			PreparedStatement updateStatement = cw.prepareStatement(update.toString());
			while(selected.next())
			{
				//update these values in toTable
				updateStatement.setLong(2, selected.getLong(1));
				Tools.copyValue(selected,2,updateStatement,1);
				int updated = updateStatement.executeUpdate();
				if(updated != 1)
				{
					throw new SQLException("Wrong number of updated rows, should be 1 but was " + updated);
				}
			}
		}

		// create new protection entry
		StringBuilder sb = new StringBuilder("UPDATE ");
		sb.append(Defaults.HAS_A_TABLENAME);
		sb.append(" SET OWNER_TABLE = ? WHERE OWNER_TABLE = ? AND RELATION_NAME = ?");
		PreparedStatement ps = cw.prepareStatement(sb.toString());
		ps.setString(1, toTable);
		ps.setString(2, fromTable);
		ps.setString(3, colName);
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
	 * Check that all objects referenced from tableName via colName are of type
	 * returnType or a subtype of return type.
	 * 
	 * If they are, update the reference.
	 * 
	 * If not, drop it.
	 * 
	 * @param tableName
	 * @param colName
	 * @param nuType
	 * @param cw
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private void updateReferences(String tableName, String colName, Class<?> currentType, Class<?> nuType, ConnectionWrapper cw)
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
			Class<?> sourceClass = ObjectTools.lookUpClass(propertyClassName, adapter);
			// check compatibility
			if (ObjectTools.isA(sourceClass, nuType))
			{
				// update the reference id
				setReferenceTo(tableName, ownerId, colName, propertyId, cw);
			}
			else
			{
				// can't convert this reference, drop it
				// null the reference in the owner table
				setReferenceTo(tableName, ownerId, colName, null, cw);
				// remove protection
				pm.unprotectObjectInternal(tableName, ownerId, propertyTable, propertyId, cw);
				// if entity is unprotected,
				if (!pm.isProtected(propertyTable, propertyId, cw))
				{
					// then delete the entity
					adapter.getPersist().deleteObject(sourceClass, propertyId, cw);
				}
			}
		}
		ps.close();
	}

	/**
	 * Set the reference stored in colname in the row in tableName with C__ID =
	 * id to nuReference.
	 * 
	 * @param tableName
	 *            the table name to update
	 * @param id
	 *            the ID_COL value of the row to update
	 * @param colName
	 *            the column to update
	 * @param nuReference
	 *            the new reference to set
	 * @param cw
	 * @throws SQLException
	 */
	private void setReferenceTo(String tableName, Long id, String colName, Long nuReference, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder statement = new StringBuilder("UPDATE ");
		statement.append(tableName);
		statement.append(" SET ");
		statement.append(colName);
		statement.append(" = ");
		if (nuReference == null)
		{
			statement.append("NULL");
		}
		else
		{
			statement.append("?");
		}
		statement.append(" WHERE ");
		statement.append(Defaults.ID_COL);
		statement.append(" = ?");
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		if (nuReference == null)
		{
			ps.setLong(1, id);
		}
		else
		{
			ps.setLong(1, nuReference);
			ps.setLong(2, id);
		}
		Tools.logFine(ps);
		ps.executeUpdate();
		ps.close();
	}

	/**
	 * @param className
	 * @param superClassName
	 * @param cw
	 * @throws SQLException
	 */
	private void addClassRelation(String className, String superClassName, ConnectionWrapper cw) throws SQLException
	{
		boolean exists = false;
		// check if the relation exists
		PreparedStatement check = cw.prepareStatement("SELECT * FROM " + Defaults.IS_A_TABLENAME + " WHERE SUBCLASS = ? AND SUPERCLASS = ?");
		check.setString(1, className);
		check.setString(2, superClassName);
		Tools.logFine(check);
		ResultSet rs = check.executeQuery();
		if (rs.next())
		{
			exists = true;
		}
		rs.close();
		if (!exists)
		{
			PreparedStatement ps = cw.prepareStatement("INSERT INTO " + Defaults.IS_A_TABLENAME + " (SUBCLASS,SUPERCLASS) values (?,?)");
			ps.setString(1, className);
			ps.setString(2, superClassName);
			Tools.logFine(ps);
			ps.execute();
			ps.close();
		}
	}

	/**
	 * Create a new column in the given table with the given name and type.
	 * 
	 * @param tableName
	 * @param columnName
	 * @param tableType
	 * @param cw
	 * @throws SQLException
	 */
	private void createColumn(String tableName, String columnName, Class<?> returnType, ConnectionWrapper cw) throws SQLException
	{
		String columnType = adapter.getColumnType(returnType, null).trim();
		PreparedStatement ps = cw.prepareStatement("ALTER TABLE " + tableName + " ADD " + columnName + " " + columnType);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
		addTypeInfo(tableName, columnName, NameGenerator.getSystemicName(returnType), cw);
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
	public void dropColumn(String tableName, String column, ConnectionWrapper cw) throws SQLException, ClassNotFoundException
	{
		dropUprotectedReferences(tableName, column, cw);
		if (adapter.canDropColumn())
		{
			StringBuilder sb = new StringBuilder("ALTER TABLE ");
			sb.append(tableName);
			sb.append(" DROP ");
			sb.append(column);
			PreparedStatement ps = cw.prepareStatement(sb.toString());
			Tools.logFine(ps);
			ps.execute();
			ps.close();
		}
		else
		{
			// The underlying database does not handle dropping columns
			// use a 4-step workaround

			// 1. rename old table to a temporary name
			String tempTableName = "T__" + tableName;
			if (adapter.tableNamesAreLowerCase())
			{
				tempTableName = tempTableName.toLowerCase();
			}
			while (tempTableName.length() > adapter.getMaximumNameLength())
			{
				tempTableName = tempTableName.substring(0, tempTableName.length() - 1);
			}
			setTableName(tableName, tempTableName, cw);

			// 2. create a new table with same columns minus the one we want to
			// remove and
			// 3. copy data from old table to new table
			cloneTableWithoutColumns(tempTableName, tableName, new String[] { column }, cw);

			// 4. drop old table
			conditionalDelete(tempTableName, cw);

		}
		removeTypeInfo(tableName, column, cw);

	}

	/**
	 * Clone a table, optionally omitting some column(s).
	 * 
	 * @param oldTableName
	 *            the table to copy data from.
	 * @param nuTableName
	 *            the new table to create and populate with data from
	 *            oldTableName.
	 * @param columnsToDrop
	 *            list of columns to omit - may be empty or null.
	 * @param cw
	 * @throws SQLException
	 */
	private void cloneTableWithoutColumns(String oldTableName, String nuTableName, String[] columnsToDrop, ConnectionWrapper cw) throws SQLException
	{

		// get the old colums
		Map<String, String> cols = this.getDatabaseColumns(oldTableName, cw);
		// remove the columns in in columnsToDrop from nuCols
		if (columnsToDrop != null)
		{
			for (String remCol : columnsToDrop)
			{
				cols.remove(remCol);
			}
		}

		// create a list of the columns to be copied
		List<String> sameColums = new ArrayList<String>();

		StringBuilder stmt = new StringBuilder("CREATE TABLE ");
		stmt.append(nuTableName);
		stmt.append(" (");
		Set<Entry<String, String>> entrySet = cols.entrySet();
		for (Entry<String, String> en : entrySet)
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
		// delete trailing comma
		stmt.deleteCharAt(stmt.length() - 1);
		stmt.append(")");
		PreparedStatement ps = cw.prepareStatement(stmt.toString());
		Tools.logFine(ps);
		ps.execute();
		ps.close();

		// check if we must commit
		if (adapter.isRequiresCommitAfterTableCreation())
		{
			cw.commit();
		}

		// copy from old table
		stmt = new StringBuilder("INSERT INTO ");
		stmt.append(nuTableName);
		stmt.append(" (");
		for (String colName : sameColums)
		{
			stmt.append(colName);
			stmt.append(",");
		}
		// delete trailing comma
		stmt.deleteCharAt(stmt.length() - 1);
		stmt.append(") SELECT ");
		for (String colName : sameColums)
		{
			stmt.append(colName);
			stmt.append(",");
		}
		// delete trailing comma
		stmt.deleteCharAt(stmt.length() - 1);
		stmt.append(" FROM ");
		stmt.append(oldTableName);
		ps = cw.prepareStatement(stmt.toString());
		Tools.logFine(ps);
		ps.execute();
		ps.close();
	}

	/**
	 * Remove protection from tableName.column for all objects. If any object is
	 * no longer protected, delete it.
	 * 
	 * @param tableName
	 * @param column
	 * @param cw
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private void dropUprotectedReferences(String tableName, String column, ConnectionWrapper cw) throws SQLException, ClassNotFoundException
	{
		// Get affected entries from HAS_A table
		StringBuilder statement = new StringBuilder("SELECT ");
		statement.append("OWNER_ID,");
		statement.append("PROPERTY_TABLE,");
		statement.append("PROPERTY_ID,");
		statement.append("PROPERTY_CLASS");
		statement.append(" FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE OWNER_TABLE=? AND ");
		statement.append(Defaults.RELATION_NAME_COL);
		statement.append("=?");

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
				if (!pm.isProtected(propertyTable, propertyId, cw))
				{
					// then delete the entity
					Class<?> c = ObjectTools.lookUpClass(propertyClassName, adapter);
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
	 * Get a map of column name -> type for the given table.
	 * 
	 * @param tableName
	 * @param cw
	 * @return
	 * @throws SQLException
	 */
	public Map<String, String> getDatabaseColumns(String tableName, ConnectionWrapper cw) throws SQLException
	{
		// TODO: Use metadata table instead
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
	 * Change the name of a table from oldName to newName. No other changes will
	 * be effected.
	 * 
	 * @param oldName
	 * @param newName
	 * @throws SQLException
	 */
	public void setTableName(String oldName, String newName, ConnectionWrapper cw) throws SQLException
	{
		if (tableExists(oldName, cw))
		{
			if (tableExists(newName, cw))
			{
				// new table exists, drop old table
				conditionalDelete(oldName, cw);
				dropAllIndicesForTable(oldName, cw);
				if (!adapter.isSupportsIdentity())
				{
					// this adapter relies on sequences, so drop the
					// corresponding
					// sequence
					String sequenceName = Tools.getSequenceName(oldName, adapter);
					String dropGeneratorQuery = "DROP GENERATOR " + sequenceName;

					PreparedStatement ps = cw.prepareStatement(dropGeneratorQuery);
					Tools.logFine(ps);
					ps.execute();
					ps.close();
				}
			}
			else
			{
				// new table does not exist, rename old table
				String[] tableRenameStmts = adapter.getTableRenameStatements(oldName, newName);
				for (String tableRenameStmt : tableRenameStmts)
				{
					PreparedStatement ps = cw.prepareStatement(tableRenameStmt);
					Tools.logFine(ps);
					ps.execute();
					ps.close();
				}
			}
		}
	}

	public void changeTypeInfo(String tableName, String propertyName, Class<?> returnType, ConnectionWrapper cw) throws SQLException
	{
		removeTypeInfo(tableName, propertyName, cw);
		addTypeInfo(tableName, propertyName, returnType, cw);
	}

	/**
	 * Add information about the return type of a given property of a given
	 * table.
	 * 
	 * @param tableName
	 * @param propertyName
	 * @param returnType
	 * @throws SQLException
	 */
	public void addTypeInfo(String tableName, String propertyName, Class<?> returnType, ConnectionWrapper cw) throws SQLException
	{
		addTypeInfo(tableName, propertyName, NameGenerator.getSystemicName(returnType), cw);
	}

	/**
	 * Add information about the return type of a given property of a given
	 * table.
	 * 
	 * @param tableName
	 * @param propertyName
	 * @param returnType
	 * @throws SQLException
	 */
	public void addTypeInfo(String tableName, String propertyName, String returnType, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder stmt = new StringBuilder("INSERT INTO ");
		stmt.append(Defaults.TYPE_TABLENAME);
		stmt.append(" (OWNER_TABLE ,COLUMN_NAME ,COLUMN_CLASS) VALUES (?,?,?)");
		PreparedStatement ps = cw.prepareStatement(stmt.toString());
		ps.setString(1, tableName);
		ps.setString(2, propertyName);
		ps.setString(3, returnType);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
	}

	/**
	 * Remove information about the return type of a given property of a given
	 * table.
	 * 
	 * @param tableName
	 * @param propertyName
	 * @throws SQLException
	 */
	public void removeTypeInfo(String tableName, String propertyName, ConnectionWrapper cw) throws SQLException
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

	/**
	 * Add a link between a class name and a table name.
	 * 
	 * @param className
	 *            the {@link NameGenerator#getSystemicName(Class)}of the class.
	 * @param tableName
	 *            the name of the table for the class.
	 * @throws SQLException
	 */
	private void setTableNameForClass(String className, String tableName, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder stmt = new StringBuilder("INSERT INTO ");
		stmt.append(Defaults.TABLE_NAME_TABLENAME);
		stmt.append(" (TABLENAME ,CLASS) VALUES (?,?)");
		PreparedStatement ps = cw.prepareStatement(stmt.toString());
		ps.setString(1, tableName);
		ps.setString(2, className);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
	}

	/**
	 * Remove the class-tablename association for a given class and/or
	 * tablename. Either className or tableName may be null, but not both.
	 * 
	 * @param className
	 *            the {@link NameGenerator#getSystemicName(Class)} of the class
	 *            to remove the association for, may be null if tableName is
	 *            non-null.
	 * @param tableName
	 *            the table name of the class to remove association for, may be
	 *            null if className is non-null.
	 * @param cw
	 * @throws SQLException
	 */
	private void removeTableNameForClass(String className, String tableName, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder stmt = new StringBuilder("DELETE FROM ");
		stmt.append(Defaults.TABLE_NAME_TABLENAME);
		stmt.append(" WHERE");
		if (className != null)
		{
			stmt.append(" CLASS=? ");
			if (tableName != null)
			{
				stmt.append("AND");
			}
		}
		if (tableName != null)
		{
			stmt.append(" TABLENAME=?");
		}
		PreparedStatement ps = cw.prepareStatement(stmt.toString());
		if (className != null)
		{
			ps.setString(1, className);
			if (tableName != null)
			{
				ps.setString(2, tableName);
			}
		}
		else
		{
			ps.setString(1, tableName);
		}
		Tools.logFine(ps);
		ps.execute();
		ps.close();
	}

	/**
	 * Get the table name for a named class.
	 * 
	 * @see NameGenerator#getTableName(Class, AdapterBase)
	 * @see NameGenerator#getSystemicName(Class)
	 * @param className
	 *            the {@link NameGenerator#getSystemicName(Class)} of the class
	 *            to find the table name for.
	 * @param cw
	 * @return the table name.
	 * @throws SQLException
	 */
	public String getTableNameForClass(String className, ConnectionWrapper cw) throws SQLException
	{
		String res = null;
		StringBuilder stmt = new StringBuilder("SELECT TABLENAME FROM ");
		stmt.append(Defaults.TABLE_NAME_TABLENAME);
		stmt.append(" WHERE CLASS=?");
		PreparedStatement ps = cw.prepareStatement(stmt.toString());
		ps.setString(1, className);
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		if (rs.next())
		{
			res = rs.getString(1);
		}
		ps.close();
		return res;
	}

	/**
	 * Get the name of the class that belongs to a given table.
	 * 
	 * @param tableName
	 *            the table name to find the class name for.
	 * @param cw
	 * @return the {@link NameGenerator#getSystemicName(Class)} of the class
	 *         that belongs to the given table.
	 * @throws SQLException
	 */
	public String getClassForTableName(String tableName, ConnectionWrapper cw) throws SQLException
	{
		String res = null;
		StringBuilder stmt = new StringBuilder("SELECT CLASS FROM ");
		stmt.append(Defaults.TABLE_NAME_TABLENAME);
		stmt.append(" WHERE TABLENAME=?");
		PreparedStatement ps = cw.prepareStatement(stmt.toString());
		ps.setString(1, tableName);
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		if (rs.next())
		{
			res = rs.getString(1);
		}
		ps.close();
		return res;
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
	private void updateAllRelations(String table, String column, String oldValue, String newValue, ConnectionWrapper cw) throws SQLException
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

	private void dropAllSubclassEntries(String superClassTable, String subClassName, ConnectionWrapper cw) throws SQLException
	{
		// delete the subclass from C__IS_A table
		StringBuilder stmt = new StringBuilder("DELETE FROM ");
		stmt.append(Defaults.IS_A_TABLENAME);
		stmt.append(" WHERE SUBCLASS=?");
		PreparedStatement ps = cw.prepareStatement(stmt.toString());
		ps.setString(1, subClassName);
		Tools.logFine(ps);
		ps.execute();
		ps.close();

		// delete C__REALCLASS from superClassTable
		stmt = new StringBuilder("UPDATE ");
		stmt.append(superClassTable);
		stmt.append(" SET C__REALCLASS  = NULL WHERE C__REALCLASS=?");
		ps = cw.prepareStatement(stmt.toString());
		ps.setString(1, subClassName);
		Tools.logFine(ps);
		ps.execute();
		ps.close();

	}

	/**
	 * Create a named table with named columns of the given types.
	 * 
	 * @param tableName
	 *            the name of the table to create.
	 * @param columnNames
	 *            the names of columns to add to the table.
	 * @param columnTypes
	 *            the SQL-types of the columns to create.
	 * @param cw
	 * @throws SQLException
	 */
	private void createTable(String tableName, String[] columnNames, String[] columnTypes, ConnectionWrapper cw) throws SQLException
	{
		if (columnNames.length != columnTypes.length)
		{
			throw new IllegalArgumentException("List of column names and column types must have equal length.");
		}
		StringBuilder create = new StringBuilder("CREATE TABLE ");
		create.append(tableName);
		create.append("(");
		for (int x = 0; x < columnNames.length; x++)
		{
			if (x > 0)
			{
				create.append(",");
			}
			create.append(columnNames[x]);
			create.append(" ");
			create.append(columnTypes[x]);
		}
		create.append(")");

		PreparedStatement ps = cw.prepareStatement(create.toString());
		Tools.logFine(ps);
		ps.execute();
		ps.close();
		if (adapter.isRequiresCommitAfterTableCreation())
		{
			cw.commit();
		}
	}

	/**
	 * Create a named index on the given columns of a named table.
	 * 
	 * @param table
	 *            the table to create the index on
	 * @param columns
	 *            an array of column names to include in the index
	 * @param indexName
	 *            the name of the index
	 * @param cw
	 * @throws SQLException
	 */
	public void createIndex(String table, String[] columns, String indexName, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder commandString = new StringBuilder("CREATE INDEX ");
		commandString.append(indexName);
		commandString.append(" on ");
		commandString.append(table);
		commandString.append("(");
		for (int x = 0; x < columns.length; x++)
		{
			if (x > 0)
			{
				commandString.append(",");
			}
			commandString.append(columns[x]);
		}
		commandString.append(")");
		PreparedStatement ps = cw.prepareStatement(commandString.toString());
		Tools.logFine(ps);
		ps.execute();
		ps.close();

		// add a row in the C__INDEX table for each link
		commandString = new StringBuilder("INSERT INTO ");
		commandString.append(Defaults.INDEX_TABLENAME);
		commandString.append("(TABLE_NAME,INDEX_NAME,COLUMN_NAME)VALUES(?,?,?)");
		for (String col : columns)
		{
			ps = cw.prepareStatement(commandString.toString());
			ps.setString(1, table);
			ps.setString(2, indexName);
			ps.setString(3, col);
			Tools.logFine(ps);
			ps.execute();
			ps.close();
		}
	}

	/**
	 * Remove a named index from a named table.
	 * 
	 * @param table
	 *            the table to remove the index from.
	 * @param indexName
	 *            the name of the index to remove.
	 * @param cw
	 * @throws SQLException
	 */
	public void dropIndex(String table, String indexName, ConnectionWrapper cw) throws SQLException
	{

		String[] statements = adapter.getDropIndexStatements(table, indexName);
		for (String statement : statements)
		{
			PreparedStatement ps = cw.prepareStatement(statement);
			Tools.logFine(ps);
			ps.execute();
			ps.close();
		}
		// remove the corresponding row(s) from the C__INDEX table
		StringBuilder commandString = new StringBuilder("DELETE FROM ");
		commandString.append(Defaults.INDEX_TABLENAME);
		commandString.append(" WHERE TABLE_NAME = ? AND INDEX_NAME = ?");
		PreparedStatement ps = cw.prepareStatement(commandString.toString());
		ps.setString(1, table);
		ps.setString(2, indexName);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
	}
}
