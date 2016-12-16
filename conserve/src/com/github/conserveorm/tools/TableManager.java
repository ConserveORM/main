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
package com.github.conserveorm.tools;

import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.github.conserveorm.Persist;
import com.github.conserveorm.adapter.AdapterBase;
import com.github.conserveorm.annotations.AsBlob;
import com.github.conserveorm.annotations.AsClob;
import com.github.conserveorm.connection.ConnectionWrapper;
import com.github.conserveorm.connection.DataConnectionPool;
import com.github.conserveorm.exceptions.SchemaPermissionException;
import com.github.conserveorm.select.All;
import com.github.conserveorm.tools.generators.NameGenerator;
import com.github.conserveorm.tools.metadata.ClassChangeList;
import com.github.conserveorm.tools.metadata.ConcreteObjectRepresentation;
import com.github.conserveorm.tools.metadata.DatabaseObjectRepresentation;
import com.github.conserveorm.tools.metadata.FieldChangeDescription;
import com.github.conserveorm.tools.metadata.InheritanceChangeCalculator;
import com.github.conserveorm.tools.metadata.InheritanceChangeDescription;
import com.github.conserveorm.tools.metadata.MetadataException;
import com.github.conserveorm.tools.metadata.ObjectRepresentation;
import com.github.conserveorm.tools.metadata.ObjectStack;
import com.github.conserveorm.tools.metadata.ObjectStack.Node;
import com.github.conserveorm.tools.protection.ProtectionManager;

import java.util.Set;

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
						new String[] { adapter.getIntegerTypeKeyword(), adapter.getLongTypeKeyword(), adapter.getIntegerTypeKeyword(),
								adapter.getIntegerTypeKeyword(), adapter.getLongTypeKeyword(), adapter.getIntegerTypeKeyword() },
						cw);

				// create an index on the tablename/id combinations, since
				// this is the one we will be searching for most frequently
				createIndex(Defaults.HAS_A_TABLENAME, new String[] { "OWNER_TABLE", "OWNER_ID" },
						Defaults.HAS_A_TABLENAME + "_OWNER_INDEX", cw);
				createIndex(Defaults.HAS_A_TABLENAME, new String[] { "PROPERTY_TABLE" , "PROPERTY_ID" },
						Defaults.HAS_A_TABLENAME + "_PROPERTY_INDEX", cw);

			}
			if (!tableExists(Defaults.ARRAY_TABLENAME, cw))
			{
				if (!this.createSchema)
				{
					throw new SchemaPermissionException(Defaults.ARRAY_TABLENAME + " does not exist, but can't create it.");
				}
				// create the table with an identity column
				createTable(Defaults.ARRAY_TABLENAME,
					new String[] { Defaults.ID_COL, Defaults.COMPONENT_TABLE_COL, Defaults.COMPONENT_CLASS_COL },
					new String[] { adapter.getLongTypeKeyword() + " PRIMARY KEY", adapter.getIntegerTypeKeyword(), adapter.getIntegerTypeKeyword() }, cw);
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
					create.append(adapter.getIntegerTypeKeyword());
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
						create.append(adapter.getIntegerTypeKeyword());
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
				createTable(Defaults.TYPE_TABLENAME, new String[] { "OWNER_TABLE", "COLUMN_NAME", "COLUMN_CLASS","COLUMN_SIZE" },
						new String[] { adapter.getVarCharIndexed(), adapter.getVarCharIndexed(), adapter.getVarCharIndexed(), adapter.getLongTypeKeyword() }, cw);

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
	public void createTriggeredSequence(ConnectionWrapper cw, String tableName) throws SQLException
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
	public boolean tableExists(ObjectRepresentation objRes, ConnectionWrapper cw) throws SQLException
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
		if (adapter.getTableNamesAreLowerCase())
		{
			tableName = tableName.toLowerCase();
			columnName = columnName.toLowerCase();
		}
		Connection c = cw.getConnection();
		String catalog = null;
		if(!adapter.getCatalogIsBroken())
		{
			catalog = c.getCatalog();
		}
		DatabaseMetaData metaData = c.getMetaData();
		ResultSet rs = metaData.getColumns(catalog, null, tableName, columnName);
		boolean res = false;
		while (rs.next())
		{
			if(rs.getString("COLUMN_NAME").equalsIgnoreCase(columnName))
			{
				res = true;
				break;
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
		if (adapter.getTableNamesAreLowerCase())
		{
			tableName = tableName.toLowerCase();
		}
		Connection c = cw.getConnection();
		String catalog = null;
		if(!adapter.getCatalogIsBroken())
		{
			catalog = c.getCatalog();
		}
		DatabaseMetaData metaData = c.getMetaData();
		ResultSet rs = metaData.getTables(catalog, null, tableName, new String[] { "TABLE" });
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

		if (!adapter.isSupportsIdentity())
		{
			if (objRes.getRepresentedClass().equals(Object.class)
					||objRes.getRepresentedClass().isArray())
			{
				createTriggeredSequence(cw, objRes.getTableName());
			}
		}

		if (adapter.isRequiresCommitAfterSchemaAlteration())
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
			//always include Object
			if(tableExists(Object.class, cw))
			{
				res.add(Object.class);
				existingClasses.add(Object.class);
			}
			// find all sub-classes
			PreparedStatement ps = cw.prepareStatement("SELECT DISTINCT(SUBCLASS) FROM " + Defaults.IS_A_TABLENAME);
			Tools.logFine(ps);
			ResultSet rs = ps.executeQuery();
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
			ps.close();
			// find all super-classes
			ps = cw.prepareStatement("SELECT DISTINCT(SUPERCLASS) FROM " + Defaults.IS_A_TABLENAME);
			Tools.logFine(ps);
			rs = ps.executeQuery();
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
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				String name = rs.getString(1);
				Class<?> c = ObjectTools.lookUpClass(name, adapter);
				res.add(c);
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
		ResultSet rs = ps.executeQuery();
		while (rs.next())
		{
			String name = rs.getString(1);
			res.add(name);
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
			adapter.getPersist().getProtectionManager().unprotectObjects(cw, adapter.getPersist().getTableNameNumberMap().getNumber(cw, tableName));

			// delete all instances of the class
			adapter.getPersist().deleteObjects(cw, c, new All());
			// drop tables of subclasses
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
				dropSequenceIfExists(tableName, cw);
			}
			// find all classes that reference c, delete them.
			ArrayList<Class<?>> referencingClasses = getReferencingClasses(c, classList);
			for (Class<?> ref : referencingClasses)
			{
				dropTableHelper(ref, cw, classList);
			}
			
			//find all interfaces implemented by c, delete those that no longer has any implementors
			List<Class<?>> interfaces = ObjectTools.getAllDirectInterfaces(c);
			for(Class<?>intf:interfaces)
			{
				if(!hasSubclasses(intf,cw))
				{
					dropTableHelper(intf, cw, classList);
				}
			}
		}
	}

	/**
	 * Return true if the class c has subclasses or implementing classes, false otherwise.
	 * @param c
	 * @param cw
	 * @return
	 * @throws SQLException 
	 */
	private boolean hasSubclasses(Class<?> c, ConnectionWrapper cw) throws SQLException
	{
		boolean res  = false;
		Connection conn = cw.getConnection();
		PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + Defaults.IS_A_TABLENAME +" WHERE SUPERCLASS = ?");
		ps.setString(1, NameGenerator.getSystemicName(c));
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		if (rs.next() && (rs.getLong(1) > 0))
		{
			res = true;
		}
		ps.close();
		return res;
	}
	

	private void conditionalDelete(String tableName, ConnectionWrapper cw) throws SQLException
	{
		if(!this.adapter.isSupportsExistsKeyword())
		{
			if(!tableExists(tableName, cw))
			{
				return;
			}
		}
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
				//this is OK
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
		Integer subClassId = adapter.getPersist().getClassNameNumberMap().getNumber(cw, subClass);

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
		stmt.setString(1, NameGenerator.getSystemicName(superClass));
		Tools.logFine(stmt);
		ResultSet tmpRes = stmt.executeQuery();
		while (tmpRes.next())
		{
			String ownerTable =tmpRes.getString(1);
			Integer ownerTableId = adapter.getPersist().getTableNameNumberMap().getNumber(cw, ownerTable);
			String relationName = tmpRes.getString(2);
			Integer relationNameId = adapter.getPersist().getColumnNameNumberMap().getNumber(cw, relationName);
			// find all entries in C__HAS_A (protection entries) where owner
			// and relation name is from the search results, and property is
			// the new subclass
			query = new StringBuilder("SELECT OWNER_ID,PROPERTY_TABLE, PROPERTY_ID FROM ");
			query.append(Defaults.HAS_A_TABLENAME);
			query.append(" WHERE OWNER_TABLE=? AND RELATION_NAME=? AND PROPERTY_CLASS=?");
			PreparedStatement innerStmt = cw.prepareStatement(query.toString());
			innerStmt.setInt(1, ownerTableId);
			innerStmt.setInt(2, relationNameId);
			innerStmt.setInt(3, subClassId);
			Tools.logFine(innerStmt);
			ResultSet innerRes = innerStmt.executeQuery();
			while (innerRes.next())
			{
				Integer propertyTableId = innerRes.getInt(2);
				// remove the reference
				setReferenceTo(ownerTable, innerRes.getLong(1), relationName, null, cw);
				// remove protection entry
				pm.unprotectObjectInternal( ownerTableId,innerRes.getLong(1), propertyTableId, innerRes.getLong(3), cw);
				// if item is unprotected, remove it
				if (!pm.isProtected(propertyTableId, innerRes.getLong(3), cw))
				{
					String propertyTableName = adapter.getPersist().getTableNameNumberMap().getName(cw, propertyTableId);
					Class<?> lookUpClass = ObjectTools.lookUpClass(getClassForTableName(propertyTableName, cw), adapter);
					adapter.getPersist().deleteObject(cw,lookUpClass, innerRes.getLong(3));
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
	 * @param klass 
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
	private void changeColumnType(Class<?> klass, String column, ObjectRepresentation fromRep, Long fromSize, ObjectRepresentation toRep, Long toSize, ConnectionWrapper cw)
			throws SQLException, ClassNotFoundException
	{
		String oldType = fromRep.getColumnType(column );
		String nuType = toRep.getColumnType(column);
		// only do conversion if it is necessary
		if (!oldType.equals(nuType))
		{
			if (adapter.canChangeColumnType())
			{
				StringBuilder sb = new StringBuilder("ALTER TABLE ");
				sb.append(toRep.getTableName());
				sb.append(" ");
				sb.append(adapter.getColumnModificationKeyword());
				sb.append(" ");
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
				renameColumn(klass,toRep.getTableName(), column, nuName, cw);
				// 2. create a new column with the desired properties
				createColumn(toRep.getTableName(), column, toRep.getReturnType(column),toRep.getColumnSize(column), cw);
				// 3. copy all values from the old to the new column
				copyValues(toRep.getTableName(), nuName, column, cw);
				// 4. drop the old column
				dropColumn(toRep.getTableName(), nuName, cw);
			}
		}

		// store the new column metadata
		changeTypeInfo(toRep.getTableName(), column, toRep.getReturnType(column),toRep.getColumnSize(column), cw);
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
	private void renameColumn(Class<?>clazz,String tableName, String oldName, String nuName, ConnectionWrapper cw) throws SQLException
	{
		// check that the old column exists
		if (adapter.getTableNamesAreLowerCase())
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

				// get the new columns
				Map<String, String> nuCols = new HashMap<String, String>();
				List<String> sameColums = new ArrayList<String>();
				nuCols.putAll(columns);
				// rename the column in nuCols
				nuCols.remove(oldName);
				sameColums.addAll(nuCols.keySet());
				// create new table, temporary name
				String temporaryName = "T__" + tableName;
				if (adapter.getTableNamesAreLowerCase())
				{
					temporaryName = temporaryName.toLowerCase();
				}
				while (temporaryName.length() > adapter.getMaximumNameLength())
				{
					temporaryName = temporaryName.substring(0, temporaryName.length() - 1);
				}
				
				ConcreteObjectRepresentation objRep=new ConcreteObjectRepresentation(adapter, clazz, null, null);
				objRep.setTableName(temporaryName);
				objRep.changeFieldName(oldName,nuName);
				String createStmt = objRep.getTableCreationStatement(cw);
				PreparedStatement ps = cw.prepareStatement(createStmt);
				Tools.logFine(ps);
				ps.execute();
				ps.close();

				// copy from old table
				StringBuilder stmt = new StringBuilder("INSERT INTO ");
				stmt.append(temporaryName);
				stmt.append(" (");
				stmt.append(Defaults.ID_COL);
				stmt.append(",");
				for (String colName : sameColums)
				{
					stmt.append(colName);
					stmt.append(",");
				}
				stmt.append(nuName);
				stmt.append(") SELECT ");
				stmt.append(Defaults.ID_COL);
				stmt.append(",");
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
				//drop entries in metadata table
				String metaUpdate = "DELETE FROM " + Defaults.TYPE_TABLENAME + " WHERE OWNER_TABLE = ?";
				PreparedStatement prepareStatement = cw.prepareStatement(metaUpdate);
				prepareStatement.setString(1, tableName);
				prepareStatement.execute();
				prepareStatement.close();
				
				// rename new table
				this.setTableName(temporaryName,clazz, tableName,clazz, cw);
				objRep.setTableName(tableName);
				createIndicesForTable(objRep, cw);
				//make sure the metadata is in order
				updateAllRelations(Defaults.TYPE_TABLENAME, "OWNER_TABLE", temporaryName, tableName, cw);
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

			Persist pers = adapter.getPersist();
			// Change name in C__HAS_A
			stmt = new StringBuilder("UPDATE ");
			stmt.append(Defaults.HAS_A_TABLENAME);
			stmt.append(" SET RELATION_NAME = ? WHERE OWNER_TABLE  = ? AND RELATION_NAME = ?");
			ps = cw.prepareStatement(stmt.toString());
			ps.setInt(1, pers.getColumnNameNumberMap().getNumber(cw, nuName));
			ps.setInt(2, pers.getTableNameNumberMap().getNumber(cw, tableName));
			ps.setInt(3, pers.getColumnNameNumberMap().getNumber(cw, oldName));
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
				for (ClassChangeList nuClasses : inheritanceChanges.getAddedSuperClasses())
				{
					if (nuClasses.size()>0)
						// add entries
						addEntries(nuObjectStack.getActual(),nuClasses, cw);
				}
				// move moved fields
				List<FieldChangeDescription> movedFields = inheritanceChanges.getMovedFields();
				for (FieldChangeDescription movedField : movedFields)
				{
					// move fields
					moveFields(movedField, cw);
				}
				// delete entries in old tables
				for (ClassChangeList deletedClasses : inheritanceChanges.getRemovedSuperClasses())
				{
					if (deletedClasses.size()>0)
						// remove entries
						removeEntries(oldObjectStack.getActual(),deletedClasses, cw);
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
						// updating references not necessary, no class should have 
						// reference to the removed class prior to removing it

						// update subclass entries
						dropAllSubclassEntries(NameGenerator.getTableName(klass, adapter), subClass, cw);

						// update protection entries
						updateAllRelations(Defaults.HAS_A_TABLENAME, 
								"PROPERTY_TABLE", 
								adapter.getPersist().getTableNameNumberMap().getNumber(cw,tableName), 
								adapter.getPersist().getTableNameNumberMap().getNumber(cw, klass),
								cw);
						updateAllRelations(Defaults.HAS_A_TABLENAME, 
								"PROPERTY_CLASS", 
								adapter.getPersist().getClassNameNumberMap().getNumber(cw,subClass), 
								adapter.getPersist().getClassNameNumberMap().getNumber(cw,klass), 
								cw);

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
								createColumn(toRep.getTableName(), change.getToName(), change.getToClass(),change.getToSize(), cw);
							}
							else if (change.isNameChange())
							{	
								renameColumn(klass,toRep.getTableName(), change.getFromName(), change.getToName(), cw);
							}
							else if (change.isTypeChange())
							{
								if (CompabilityCalculator.calculate(change.getFromClass(), change.getToClass()))
								{
									// there is a conversion available
									// change the column type
									changeColumnType(klass,change.getToName(),fromRep,change.getFromSize(),toRep,change.getToSize(),cw);
									
									// Update object references and remove
									// incompatible entries
									Integer tableNameId = adapter.getPersist().getTableNameNumberMap().getNumber(cw, toRep.getTableName());
									updateReferences(tableNameId, change.getToName(), change.getFromClass(), change.getToClass(), cw);
								}
								else
								{
									// no conversion, drop and recreate.
									dropColumn(toRep.getTableName(), change.getFromName(), cw);
									createColumn(toRep.getTableName(), change.getToName(), change.getToClass(),change.getToSize(), cw);
								}
							} 
							else //these changes can occur at the same time
							{
								if(change.isSizeChange())
								{
									//resize the column
									changeColumnType(klass,change.getToName(),fromRep,change.getFromSize(),toRep,change.getToSize(),cw);
								}
								if (change.isIndexChange())
								{
									// indexes have changed
									recreateIndices(toRep, cw);
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
		}
		else
		{
			throw new SchemaPermissionException("We do not have permission to change the database schema.");
		}
	}

	/**
	 * Check if the sequence for a table exists, drop it if it does.
	 * @param tableName
	 * @throws SQLException 
	 */
	private void dropSequenceIfExists(String tableName,ConnectionWrapper cw) throws SQLException
	{
		String sequenceName = Tools.getSequenceName(tableName, adapter);
		
		//get the  statement to find out if the sequence exists
		String sequenceExistsQuery = adapter.getSequenceExistsStatement(sequenceName);
		PreparedStatement existStmt = cw.prepareStatement(sequenceExistsQuery);
		Tools.logFine(existStmt);
		ResultSet rs = existStmt.executeQuery();
		if(rs.next() && rs.getInt(1)>=1)
		{
			String dropGeneratorQuery = "DROP GENERATOR " + sequenceName;
			PreparedStatement ps = cw.prepareStatement(dropGeneratorQuery);
			Tools.logFine(ps);
			ps.execute();
			ps.close();
		}
		existStmt.close();
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
			Tools.logFine("Column \""+movedField.getToTable()+"."+movedField.getToName()+"\" does not exist, creating it.");
			createColumn(movedField.getToTable(), movedField.getToName(), movedField.getToClass(),movedField.getToSize(), cw);
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

		Persist pers = adapter.getPersist();
		// move protection entries
		// move table names
		StringBuilder sb = new StringBuilder("UPDATE ");
		sb.append(Defaults.HAS_A_TABLENAME);
		sb.append(" SET OWNER_TABLE = ? WHERE OWNER_TABLE = ? AND RELATION_NAME = ?");
		PreparedStatement ps = cw.prepareStatement(sb.toString());
		ps.setInt(1, pers.getTableNameNumberMap().getNumber(cw, movedField.getToTable()));
		ps.setInt(2, pers.getTableNameNumberMap().getNumber(cw, movedField.getFromTable()));
		ps.setInt(3, pers.getColumnNameNumberMap().getNumber(cw, movedField.getFromName()));
		ps.executeUpdate();
		ps.close();

		// move field names
		sb = new StringBuilder("UPDATE ");
		sb.append(Defaults.HAS_A_TABLENAME);
		sb.append(" SET RELATION_NAME = ? WHERE OWNER_TABLE = ? AND RELATION_NAME = ?");
		ps = cw.prepareStatement(sb.toString());
		ps.setInt(1, pers.getColumnNameNumberMap().getNumber(cw, movedField.getToName()));
		ps.setInt(2, pers.getTableNameNumberMap().getNumber(cw, movedField.getToTable()));
		ps.setInt(3, pers.getColumnNameNumberMap().getNumber(cw, movedField.getFromName()));
		ps.executeUpdate();
		ps.close();

		// old class doesn't need this field anymore
		if(movedField.getFromClass()==null)
		{
			dropColumn(movedField.getFromTable(), movedField.getFromName(), cw);
		}

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
	private void addEntries(Node baseClass,ClassChangeList nuClasses, ConnectionWrapper cw) throws SQLException, SchemaPermissionException
	{
		// make sure all tables exists
		for (int tableIdx = 0; tableIdx < nuClasses.size(); tableIdx++)
		{
			Node n = nuClasses.getNode(tableIdx);
			ensureTableExists((ConcreteObjectRepresentation) n.getRepresentation(), cw);
		}

		// update the C_IS_A table to reflect new superclass
		for (int tableIdx = 1; tableIdx < nuClasses.size(); tableIdx++)
		{
			Node sub = nuClasses.getNode(tableIdx-1);
			Node sup = nuClasses.getNode(tableIdx);
			addClassRelation(sub.getRepresentation().getSystemicName(), sup.getRepresentation().getSystemicName(), cw);
		}

		// get the id of all entries in the fist class
		String baseTable = baseClass.getRepresentation().getTableName();
		StringBuilder query = new StringBuilder();
		query.append("SELECT ");
		query.append(Defaults.ID_COL);
		query.append(" FROM ");
		query.append(baseTable);
		PreparedStatement ps = cw.prepareStatement(query.toString());
		Tools.logFine(ps);
		//find out how far up the list to go
		//if the list is shared, stop one step early and just re-point the remaining nodes
		int stop = nuClasses.isShared()?nuClasses.size()-1:nuClasses.size();
		ResultSet rs = ps.executeQuery();
		while (rs.next())
		{
			long id = rs.getLong(1);
			for (int x = 1; x < stop; x++)
			{
				Node nuClass = nuClasses.getNode(x);
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
				String className = nuClasses.getNode(x-1).getRepresentation().getSystemicName();
				Integer classNameId = adapter.getPersist().getClassNameNumberMap().getNumber(cw, className);
				pInsert.setInt(2, classNameId);
				Tools.logFine(pInsert);
				pInsert.executeUpdate();
				pInsert.close();
			}
		}
		if(nuClasses.isShared())
		{
			Node sharedNode = nuClasses.getNode(nuClasses.size()-1);
			StringBuilder reName = new StringBuilder();
			reName.append("UPDATE ");
			reName.append(sharedNode.getRepresentation().getTableName());
			reName.append(" SET ");
			reName.append(Defaults.REAL_CLASS_COL);
			reName.append(" = ? WHERE " );
			reName.append(Defaults.REAL_CLASS_COL);
			reName.append(" = ?");
			PreparedStatement pRename = cw.prepareStatement(reName.toString());
			String toClassName = nuClasses.getSharedSub().getRepresentation().getSystemicName();
			Integer toClassId = adapter.getPersist().getClassNameNumberMap().getNumber(cw, toClassName);
			pRename.setInt(1, toClassId);
			String fromClassName = nuClasses.getSharedSub().getRepresentation().getSystemicName();
			Integer fromClassId = adapter.getPersist().getClassNameNumberMap().getNumber(cw, fromClassName);
			pRename.setInt(2, fromClassId);
			Tools.logFine(pRename);
			pRename.executeUpdate();
			pRename.close();
		}
		ps.close();

	}

	/**
	 * 
	 * @param deletedClasses
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private void removeEntries(Node baseClass,ClassChangeList deletedClasses, ConnectionWrapper cw) throws ClassNotFoundException, SQLException
	{

		// get the id of all entries in the first class
		String baseTable = baseClass.getRepresentation().getTableName();
		StringBuilder query = new StringBuilder();
		query.append("SELECT ");
		query.append(Defaults.ID_COL);
		query.append(" FROM ");
		query.append(baseTable);
		PreparedStatement ps = cw.prepareStatement(query.toString());
		Tools.logFine(ps);
		int stop = deletedClasses.isShared()?deletedClasses.size()-1:deletedClasses.size();
		ResultSet rs = ps.executeQuery();
		while (rs.next())
		{
			long id = rs.getLong(1);
			for (int x = 1; x < stop; x++)
			{
				Node nuClass = deletedClasses.getNode(x);
				// remove the entries
				StringBuilder deleteStmt = new StringBuilder();
				deleteStmt.append("DELETE FROM ");
				deleteStmt.append(nuClass.getRepresentation().getTableName());
				deleteStmt.append(" WHERE ");
				deleteStmt.append(Defaults.ID_COL);
				deleteStmt.append(" = ?");
				PreparedStatement pDelete = cw.prepareStatement(deleteStmt.toString());
				pDelete.setLong(1, id);
				Tools.logFine(pDelete);
				pDelete.executeUpdate();
				pDelete.close();

			}
		}
		ps.close();

		// remove the immediate super-class IS_A relationship
		Node sup = deletedClasses.getNode(1);
		deleteClassRelation(sup.getRepresentation().getRepresentedClass(), baseClass.getRepresentation().getRepresentedClass(), cw);

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
	 * Check that all objects referenced from tableName via colName are of type
	 * returnType or a subtype of return type.
	 * 
	 * If they are, update the reference.
	 * 
	 * If not, drop it.
	 * 
	 * @param tableNameId
	 * @param colName
	 * @param nuType
	 * @param cw
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private void updateReferences(Integer tableNameId, String colName, Class<?> currentType, Class<?> nuType, ConnectionWrapper cw)
			throws SQLException, ClassNotFoundException
	{
		String tableName = adapter.getPersist().getTableNameNumberMap().getName(cw, tableNameId);
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
		ps.setInt(1, tableNameId);
		Integer colNameId = adapter.getPersist().getColumnNameNumberMap().getNumber(cw, colName);
		ps.setInt(2, colNameId);
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		ProtectionManager pm = adapter.getPersist().getProtectionManager();
		while (rs.next())
		{
			// get data on one instance
			Long ownerId = rs.getLong(1);
			Integer propertyTableId = rs.getInt(2);
			Long propertyId = rs.getLong(3);
			Integer propertyClassNameId = rs.getInt(4);
			String propertyClassName = adapter.getPersist().getClassNameNumberMap().getName(cw, propertyClassNameId);
			Class<?> sourceClass = ObjectTools.lookUpClass(propertyClassName, adapter);
			// check compatibility
			if (nuType.isAssignableFrom(sourceClass))
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
				pm.unprotectObjectInternal(tableNameId, ownerId, propertyTableId, propertyId, cw);
				// if entity is unprotected,
				if (!pm.isProtected(propertyTableId, propertyId, cw))
				{
					// then delete the entity
					adapter.getPersist().deleteObject(cw,sourceClass, propertyId);
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
	private void createColumn(String tableName, String columnName, Class<?> returnType, Long size,ConnectionWrapper cw) throws SQLException
	{
		String columnType = adapter.getColumnType(returnType, null).trim();
		PreparedStatement ps = cw.prepareStatement("ALTER TABLE " + tableName + " ADD " + columnName + " " + columnType);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
		addTypeInfo(tableName, columnName, NameGenerator.getSystemicName(returnType),size, cw);
		if(adapter.isRequiresCommitAfterSchemaAlteration())
		{
			cw.commit();
		}
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
		Integer tableNameId = adapter.getPersist().getTableNameNumberMap().getNumber(cw, tableName);
		dropUprotectedReferences(tableNameId, column, cw);
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
			if (adapter.getTableNamesAreLowerCase())
			{
				tempTableName = tempTableName.toLowerCase();
			}
			while (tempTableName.length() > adapter.getMaximumNameLength())
			{
				tempTableName = tempTableName.substring(0, tempTableName.length() - 1);
			}
			setTableName(tableName,null, tempTableName,null, cw);
			//make sure metadata is correctly updated
			updateAllRelations(Defaults.TYPE_TABLENAME, "OWNER_TABLE", tableName, tempTableName, cw);

			// 2. create a new table with same columns minus the one we want to
			// remove and
			// 3. copy data from old table to new table
			cloneTableWithoutColumns(tempTableName, tableName, new String[] { column }, cw);

			// 4. drop old table
			conditionalDelete(tempTableName, cw);
			// put the metadata back in order
			updateAllRelations(Defaults.TYPE_TABLENAME, "OWNER_TABLE", tempTableName, tableName, cw);

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
		// remove the columns in columnsToDrop from nuCols
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
		if (adapter.isRequiresCommitAfterSchemaAlteration())
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
	 * @param tableNameId
	 * @param column
	 * @param cw
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private void dropUprotectedReferences(Integer tableNameId, String column, ConnectionWrapper cw) throws SQLException, ClassNotFoundException
	{
		Integer columnNameId = adapter.getPersist().getColumnNameNumberMap().getNumber(cw, column);
		// Get affected entries from HAS_A table
		StringBuilder statement = new StringBuilder("SELECT OWNER_ID,PROPERTY_TABLE,PROPERTY_ID,PROPERTY_CLASS FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE OWNER_TABLE=? AND ");
		statement.append(Defaults.RELATION_NAME_COL);
		statement.append("=?");

		PreparedStatement ps = cw.prepareStatement(statement.toString());
		ps.setInt(1, tableNameId);
		ps.setInt(2, columnNameId);
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		ProtectionManager pm = adapter.getPersist().getProtectionManager();
		while (rs.next())
		{
			// get data on one instance
			Long ownerId = rs.getLong(1);
			Integer propertyTableId = rs.getInt(2);
			Long propertyId = rs.getLong(3);
			Integer propertyClassNameId = rs.getInt(4);
			if (!rs.wasNull())
			{
				// remove protection
				pm.unprotectObjectInternal( tableNameId,ownerId, propertyTableId, propertyId, cw);
				// if entity is unprotected,
				if (!pm.isProtected(propertyTableId, propertyId, cw))
				{
					// then delete the entity
					String propertyClassName = adapter.getPersist().getClassNameNumberMap().getName(cw, propertyClassNameId);
					Class<?> c = ObjectTools.lookUpClass(propertyClassName, adapter);
					adapter.getPersist().deleteObject(cw,c, propertyId);
				}
			}
			else
			{
				// we're dealing with an array, delete it especially
				String propertyTable = adapter.getPersist().getTableNameNumberMap().getName(cw, propertyTableId);
				adapter.getPersist().deleteObject(cw,propertyTable, propertyId);
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
	 * @throws SQLException
	 */
	public Map<String, String> getDatabaseColumns(String tableName, ConnectionWrapper cw) throws SQLException
	{
		
		Map<String,String>res = new HashMap<>();
		res.put(Defaults.ID_COL, adapter.getLongTypeKeyword());
		res.put(Defaults.REAL_CLASS_COL, adapter.getIntegerTypeKeyword());
		String query = "SELECT COLUMN_NAME,COLUMN_CLASS FROM "+Defaults.TYPE_TABLENAME+" WHERE  OWNER_TABLE=?";
		PreparedStatement prepareStatement = cw.prepareStatement(query);
		prepareStatement.setString(1, tableName);
		Tools.logFine(prepareStatement);
		ResultSet rs = prepareStatement.executeQuery();
		while (rs.next())
		{
			String columnName = rs.getString(1);
			String className = rs.getString(2);
			try
			{
				String type = adapter.getColumnType(ObjectTools.lookUpClass(className, adapter), null);
				res.put(columnName, type);
			}
			catch (ClassNotFoundException e)
			{
				throw new SQLException(e);
			}
		}
		prepareStatement.close();
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
	public void setTableName( String oldName, Class<?> oldClass, String newName, Class<?>newClass, ConnectionWrapper cw) throws SQLException
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
					// corresponding sequence
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
				if(adapter.indicesMustBeManuallyDropped())
				{
					dropAllIndicesForTable(oldName, cw);
				}
				if(!adapter.canRenameTable())
				{
					//we can't rename the table. We have to create a new, identical table. 
					//The call to getTableRenameStatements() below will handle copying values.
					ConcreteObjectRepresentation objRep = new ConcreteObjectRepresentation(adapter, oldClass,null,null);
					objRep.setTableName(newName);
					if(!tableExists(objRep, cw))
					{

						String createStatement = objRep.getTableCreationStatement(cw);

						PreparedStatement ps = cw.prepareStatement(createStatement);
						Tools.logFine(ps);
						ps.execute();
						ps.close();

						if (adapter.isRequiresCommitAfterSchemaAlteration())
						{
							cw.commit();
						}
					}
				}
				String[] tableRenameStmts = adapter.getTableRenameStatements(oldName, oldClass, newName,newClass);
				for (String tableRenameStmt : tableRenameStmts)
				{
					PreparedStatement ps = cw.prepareStatement(tableRenameStmt);
					Tools.logFine(ps);
					ps.execute();
					ps.close();
				}
				
				if(adapter.indicesMustBeRecreatedAfterRename())
				{
					//find the indices of the old table in the metadata table, apply them to new table
					CaseInsensitiveStringMap map = new CaseInsensitiveStringMap();
					StringBuilder sb = new StringBuilder("SELECT COLUMN_NAME,INDEX_NAME FROM ");
					sb.append(Defaults.INDEX_TABLENAME);
					sb.append(" WHERE TABLE_NAME = ?");
					PreparedStatement ps = cw.prepareStatement(sb.toString());
					ps.setString(1, oldName);
					ResultSet rs = ps.executeQuery();
					while(rs.next())
					{
						String col = rs.getString(1);
						String idx = rs.getString(2);
						String existing = map.get(idx);
						if(existing == null)
						{
							map.put(idx, col);
						}
						else
						{
							map.put(idx, existing+","+col);
						}
					}
					ps.close();
					//add the indices to the table
					for(Map.Entry<String,String>e:map.entrySet())
					{
						StringBuilder commandString = new StringBuilder("CREATE INDEX ");
						commandString.append(e.getKey());
						commandString.append(" on ");
						commandString.append(newName);
						commandString.append("(");
						commandString.append(e.getValue());
						commandString.append(")");
						PreparedStatement createIndexStmt = cw.prepareStatement(commandString.toString());
						Tools.logFine(createIndexStmt);
						createIndexStmt.execute();
						createIndexStmt.close();
					}
				}
			}
		}
	}

	public void changeTypeInfo(String tableName, String propertyName, Class<?> returnType,Long size, ConnectionWrapper cw) throws SQLException
	{
		removeTypeInfo(tableName, propertyName, cw);
		addTypeInfo(tableName, propertyName, returnType,size, cw);
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
	public void addTypeInfo(String tableName, String propertyName, Class<?> returnType, Long size, ConnectionWrapper cw) throws SQLException
	{
		addTypeInfo(tableName, propertyName, NameGenerator.getSystemicName(returnType),size, cw);
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
	public void addTypeInfo(String tableName, String propertyName, String returnType, Long size, ConnectionWrapper cw) throws SQLException
	{
		StringBuilder stmt = new StringBuilder("INSERT INTO ");
		stmt.append(Defaults.TYPE_TABLENAME);
		stmt.append(" (OWNER_TABLE ,COLUMN_NAME ,COLUMN_CLASS, COLUMN_SIZE) VALUES (?,?,?,?)");
		PreparedStatement ps = cw.prepareStatement(stmt.toString());
		ps.setString(1, tableName);
		ps.setString(2, propertyName);
		ps.setString(3, returnType);
		if(size !=null)
		{
			ps.setLong(4, size);
		}
		else
		{
			ps.setNull(4, Types.BIGINT );
		}
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
	private void updateAllRelations(String table, String column, Integer oldValue, Integer newValue, ConnectionWrapper cw) throws SQLException
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
		stmt.append(" SET " + Defaults.REAL_CLASS_COL +" = NULL WHERE "+Defaults.REAL_CLASS_COL+" = ?");
		ps = cw.prepareStatement(stmt.toString());
		Integer subClassNameId = adapter.getPersist().getClassNameNumberMap().getNumber(cw, subClassName);
		ps.setInt(1, subClassNameId);
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
		if (adapter.isRequiresCommitAfterSchemaAlteration())
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
