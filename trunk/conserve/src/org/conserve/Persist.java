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
package org.conserve;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.conserve.adapter.AdapterBase;
import org.conserve.adapter.DerbyAdapter;
import org.conserve.adapter.FirebirdAdapter;
import org.conserve.adapter.HsqldbAdapter;
import org.conserve.adapter.MonetDbAdapter;
import org.conserve.adapter.MySqlAdapter;
import org.conserve.adapter.PostgreSqlAdapter;
import org.conserve.adapter.SqLiteAdapter;
import org.conserve.cache.ObjectRowMap;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.connection.DataConnectionPool;
import org.conserve.exceptions.SchemaPermissionException;
import org.conserve.select.Clause;
import org.conserve.select.StatementPrototypeGenerator;
import org.conserve.tools.ArrayEntryWriter;
import org.conserve.tools.ArrayLoader;
import org.conserve.tools.ClassIdTuple;
import org.conserve.tools.Defaults;
import org.conserve.tools.DelayedInsertionBuffer;
import org.conserve.tools.Duplicator;
import org.conserve.tools.MapEntry;
import org.conserve.tools.NameGenerator;
import org.conserve.tools.ObjectFactory;
import org.conserve.tools.ObjectRepresentation;
import org.conserve.tools.ObjectStack;
import org.conserve.tools.ObjectTools;
import org.conserve.tools.StatementPrototype;
import org.conserve.tools.TableManager;
import org.conserve.tools.Tools;
import org.conserve.tools.Updater;
import org.conserve.tools.protection.ProtectionManager;

/**
 * Object database interface backend. Handles the requests issued by
 * PersistenceManager.
 * 
 * 
 * @author Erik Berglund
 * 
 */
public class Persist
{
	private DataConnectionPool connectionPool;

	private boolean createSchema;

	/**
	 * Default adapter object.
	 */
	private AdapterBase adapter;

	/**
	 * Keeps track of what objects are already in the database.
	 */
	private ObjectRowMap cache = new ObjectRowMap();

	private TableManager tableManager;

	private ProtectionManager protectionManager;

	private Updater updater;

	private ArrayEntryWriter arrayEntryWriter;

	private String connectionString;

	private static final Logger LOGGER = Logger.getLogger("org.conserve");

	/**
	 * Package-access constructor.
	 */
	Persist()
	{

	}

	void initialize(Properties prop) throws SQLException
	{
		String driver = prop.getProperty("org.conserve.driver");
		String connectionString = prop
				.getProperty("org.conserve.connectionstring");
		if (connectionString == null)
		{
			throw new SQLException(
					"Property org.conserve.connectionstring not found.");
		}
		String userName = prop.getProperty("org.conserve.username");
		if (userName == null)
		{
			throw new SQLException("Property org.conserve.username not found.");
		}
		String password = prop.getProperty("org.conserve.password");
		if (password == null)
		{
			throw new SQLException("Property org.conserve.password not found.");
		}
		initialize(driver, connectionString, userName, password);
		cache.start();
	}

	void initialize(String driver, String connectionstring, String username,
			String password) throws SQLException
	{
		this.connectionString = connectionstring;
		// create the pool
		connectionPool = new DataConnectionPool(1, driver, connectionstring,
				username, password);
		// set up the default adapter
		adapter = selectAdapter(connectionstring);
		LOGGER.fine("Selected adapter: "
				+ ObjectTools.getSystemicName(adapter.getClass())
				+ " for connection " + connectionstring);
		// set up a new protection manager
		protectionManager = new ProtectionManager();
		// set up the object responsible for updating objects
		updater = new Updater(adapter);

		arrayEntryWriter = new ArrayEntryWriter(adapter);

		// set up a manager for system tables and table creation
		tableManager = new TableManager(this.isCreateSchema(), connectionPool,
				adapter);
		try
		{
			tableManager.initializeSystemTables();
		}
		catch (SchemaPermissionException e)
		{
			throw new SQLException(e);
		}
	}

	/**
	 * Select the appropriate AdapterBase implementation for a given driver.
	 * 
	 * @param driver
	 * @return
	 */
	private AdapterBase selectAdapter(String driver)
	{
		AdapterBase res = null;
		if (driver.startsWith("jdbc:mysql:"))
		{
			res = new MySqlAdapter(this);
		}
		else if (driver.startsWith("jdbc:postgresql:"))
		{
			res = new PostgreSqlAdapter(this);
		}
		else if (driver.startsWith("jdbc:derby:"))
		{
			res = new DerbyAdapter(this);
		}
		else if (driver.startsWith("jdbc:hsqldb:"))
		{
			res = new HsqldbAdapter(this);
		}
		else if (driver.startsWith("jdbc:firebirdsql:"))
		{
			res = new FirebirdAdapter(this);
		}
		else if (driver.startsWith("jdbc:sqlite:"))
		{
			res = new SqLiteAdapter(this);
		}
		else if (driver.startsWith("jdbc:monetdb"))
		{
			res = new MonetDbAdapter(this);
		}
		// TODO: If you want to extend Conserve to handle a new RDBMS,
		// add the case here and return a custom subclass of AdapterBase.
		else
		{
			// use the default (H2) adapter.
			res = new AdapterBase(this);
		}
		return res;
	}

	/**
	 * Indicate whether tables and indices should be automatically created.
	 * 
	 * @param createSchema
	 */
	public void setCreateSchema(boolean createSchema)
	{
		this.createSchema = createSchema;
		if (this.tableManager != null)
		{
			tableManager.setCreateSchema(createSchema);
		}
	}

	/**
	 * Check if this object automatically creates schema as needed.
	 * 
	 * @return true if tables are automatically created.
	 */
	public boolean isCreateSchema()
	{
		return this.createSchema;
	}

	/**
	 * Get a wrapped SQL connection to the database used by this Persist
	 * instance. To use the connection, call the getConnection() method on the
	 * returned object.
	 * 
	 * @return a ready-to-use ConnectionWrapper object.
	 * @throws SQLException
	 */
	public ConnectionWrapper getConnectionWrapper() throws SQLException
	{
		return this.connectionPool.getConnectionWrapper();
	}

	/**
	 * Delete all objects of class clazz (or any of its subclasses) that satisfy
	 * the where clause. If clazz is an interface, delete all implementing
	 * classes that satisfy the where clause.
	 * 
	 * Convenience method that does not require the user to supply a
	 * ConnectionWrapper.
	 * 
	 * @param clazz
	 * @param where
	 * @return the number of deleted objects.
	 */
	<T> int deleteObjects(Class<T> clazz, Clause where) throws SQLException
	{
		ConnectionWrapper cw = connectionPool.getConnectionWrapper();
		int res = 0;
		try
		{
			res = deleteObjects(cw, clazz, where);
			cw.commitAndDiscard();
		}
		catch (Exception e)
		{
			cw.rollbackAndDiscard();
			throw new SQLException(e);
		}
		return res;
	}

	/**
	 * Delete all objects of class clazz (or any of its subclasses) that satisfy
	 * the where clause. If clazz is an interface, delete all implementing
	 * classes that satisfy the where clause.
	 * 
	 * @param clazz
	 *            the class of objects to delete.
	 * @param where
	 *            the clause that must be satisfied for an object to be deleted.
	 * @param cw
	 *            the connection wrapper to use for this operation.
	 * @return the number of deleted objects.
	 */
	public <T> int deleteObjects(ConnectionWrapper cw, Class<T> clazz,
			Clause where) throws SQLException
	{
		int res = 0;
		try
		{
			if (!clazz.isInterface())
			{
				res = deleteObjectsNonInterface(cw, clazz, where);
			}
			else
			{
				// get all implementing classes, too
				List<Class<? extends T>> subClasses = getImplementingClasses(
						clazz, cw);
				for (Class<? extends T> subClass : subClasses)
				{
					// recurse for all all implementing classes
					res += deleteObjects(cw, subClass, where);
				}
			}
		}
		catch (ClassNotFoundException e)
		{
			throw new SQLException(e);
		}
		return res;
	}

	private int deleteObjectsNonInterface(ConnectionWrapper cw, Class<?> clazz,
			Clause where) throws ClassNotFoundException, SQLException
	{
		int deletedCount = 0;
		String className = ObjectTools.getSystemicName(clazz);
		// find all matching objects
		HashMap<Class<?>, ArrayList<Long>> objectDescr = getObjectDescriptors(
				clazz, className, where, null, cw);
		for (Entry<Class<?>, ArrayList<Long>> en : objectDescr.entrySet())
		{
			Class<?> c = en.getKey();
			String tableName = NameGenerator.getTableName(c, adapter);
			ArrayList<Long> ids = en.getValue();
			for (Long id : ids)
			{
				// delete objects from cache
				cache.purge(tableName, id);
				// unprotect the objects
				protectionManager.unprotectObjectExternal(tableName, id, cw);
				// check if objects are part of other object
				if (!protectionManager.isProtected(tableName, id, cw))
				{
					// if not, delete
					deleteObject(c, id, cw);
					deletedCount++;

				}
			}
		}
		return deletedCount;
	}

	/**
	 * Delete an object, recursively clearing corresponding entries in
	 * superclass tables.
	 * 
	 * @param clazz
	 *            the type of object to delete.
	 * @param id
	 *            the database id to delete.
	 * @param cw
	 *            the ConnectionWrapper to use for this transaction.
	 * @throws SQLException
	 */
	public void deleteObject(Class<?> clazz, Long id, ConnectionWrapper cw)
			throws SQLException
	{
		String tableName = NameGenerator.getTableName(clazz, adapter);
		deleteObject(tableName, id, cw);
		deleteObject(clazz.getSuperclass(), ObjectTools.getSystemicName(clazz),
				id, cw);
	}

	public void deleteObject(String tableName, Long id, ConnectionWrapper cw)
			throws SQLException
	{
		boolean isArray = false;
		if (tableName.equals(Defaults.ARRAY_TABLE_NAME))
		{
			// this means the object is an array, delete the array
			// delete all the array's members before deleting the array itself
			deletePropertiesOf(tableName, id, cw);
			// all arrays also have an entry in the java.lang.Object table, so
			// we must delete this as well
			deleteObject(java.lang.Object.class, Defaults.ARRAY_TABLE_NAME, id,
					cw);
			isArray = true;
		}
		StringBuilder statement = new StringBuilder("DELETE FROM ");
		statement.append(tableName);
		statement.append(" WHERE ");
		statement.append(Defaults.ID_COL);
		statement.append(" = ?");
		PreparedStatement ps = cw.prepareStatement(statement.toString());
		ps.setLong(1, id);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
		if (!isArray)
		{
			// properties of non-arrays are deleted after the object itself
			deletePropertiesOf(tableName, id, cw);
		}
	}

	private void deleteObject(Class<?> clazz, String realClassName,
			Long realId, ConnectionWrapper cw) throws SQLException
	{
		if (clazz != null)
		{
			String tableName = NameGenerator.getTableName(clazz, adapter);
			// get the id of this instance at the current level (one above
			// realClass)
			Long currentId = getId(clazz, realClassName, realId, cw);
			// delete the instance
			StringBuilder statement = new StringBuilder("DELETE FROM ");
			statement.append(tableName);
			statement.append(" WHERE ");
			statement.append(Defaults.REAL_ID_COL);
			statement.append(" = ? AND ");
			statement.append(Defaults.REAL_CLASS_COL);
			statement.append(" = ? ");
			PreparedStatement ps = cw.prepareStatement(statement.toString());
			ps.setLong(1, realId);
			ps.setString(2, realClassName);
			Tools.logFine(ps);
			ps.execute();
			ps.close();
			// recurse
			if (currentId != null)
			{
				deleteObject(clazz.getSuperclass(),
						ObjectTools.getSystemicName(clazz), currentId, cw);
				deletePropertiesOf(tableName, currentId, cw);
			}
		}
	}

	/**
	 * Recursively delete the properties of the object of clazz with id.
	 * 
	 * @param tableName
	 *            the table of the object who's properties we are deleting.
	 * @param id
	 *            the id of the object who's properties we are deleting.
	 * @throws SQLException
	 */
	private void deletePropertiesOf(String tableName, Long id,
			ConnectionWrapper cw) throws SQLException
	{
		// find all properties
		StringBuilder statement = new StringBuilder(
				"SELECT PROPERTY_TABLE, PROPERTY_ID, PROPERTY_CLASS FROM ");
		statement.append(Defaults.HAS_A_TABLENAME);
		statement.append(" WHERE OWNER_TABLE = ? AND OWNER_ID = ?");
		PreparedStatement query = cw.prepareStatement(statement.toString());
		query.setString(1, tableName);
		query.setLong(2, id);
		Tools.logFine(query);
		ResultSet subObjects = query.executeQuery();
		while (subObjects.next())
		{
			String propertyTable = subObjects.getString(1);
			Long propertyId = subObjects.getLong(2);
			String propertyClassName = subObjects.getString(3);
			// unprotect the sub-object relative to the current object
			protectionManager.unprotectObjectInternal(tableName, id,
					propertyTable, propertyId, cw);
			// check if the property is unprotected
			if (!protectionManager.isProtected(propertyTable, propertyId, cw))
			{
				// delete the property itself
				try
				{
					if (propertyTable.equals(Defaults.ARRAY_TABLE_NAME)
							|| propertyTable
									.startsWith(Defaults.ARRAY_MEMBER_TABLE_NAME)
							|| propertyClassName.contains("["))
					{
						deleteObject(propertyTable, propertyId, cw);
					}
					else
					{
						Class<?> c = ObjectTools.lookUpClass(propertyClassName);
						deleteObject(c, propertyId, cw);
					}
				}
				catch (ClassNotFoundException e)
				{
					query.close();
					throw new SQLException(e);
				}
			}
		}
		query.close();
	}

	/**
	 * Get the id of the entry in the the table associated with clazz that has
	 * the given realId and realClassName.
	 * 
	 * @param clazz
	 * @param realClassName
	 * @param realId
	 * @return
	 * @throws SQLException
	 */
	private Long getId(Class<?> clazz, String realClassName, Long realId,
			ConnectionWrapper ec) throws SQLException
	{
		String tableName = NameGenerator.getTableName(clazz, adapter);
		StringBuilder statement = new StringBuilder("SELECT ");
		statement.append(Defaults.ID_COL);
		statement.append(" FROM ");
		statement.append(tableName);
		statement.append(" WHERE ");
		statement.append(Defaults.REAL_ID_COL);
		statement.append(" = ? AND ");
		statement.append(Defaults.REAL_CLASS_COL);
		statement.append(" = ? ");
		PreparedStatement ps = ec.prepareStatement(statement.toString());
		ps.setLong(1, realId);
		ps.setString(2, realClassName);
		Tools.logFine(ps);
		try
		{
			ResultSet rs = ps.executeQuery();
			if (rs.next())
			{
				return rs.getLong(1);
			}
			else
			{
				// nothing found
				return null;
			}
		}
		finally
		{
			ps.close();
		}
	}

	/**
	 * Get a list of the classes and ids that satisfy the clause.
	 * 
	 * Either clazz or className must be non-null. If both are non-null they
	 * must refer to the same class, so that clazz.getCanonincaName() ==
	 * className.
	 * 
	 * @param clazz
	 * @param className
	 * @param clause
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	private <T> HashMap<Class<?>, ArrayList<Long>> getObjectDescriptors(
			Class<T> clazz, String className, Clause clause, Long id,
			ConnectionWrapper cw) throws ClassNotFoundException, SQLException
	{
		HashMap<Class<?>, ArrayList<Long>> res = new HashMap<Class<?>, ArrayList<Long>>();
		if (className != null && className.equals(Defaults.ARRAY_TABLE_NAME))
		{
			// can't load array classes, so ignore
			return res;
		}
		if (clazz == null)
		{
			clazz = (Class<T>) ClassLoader.getSystemClassLoader().loadClass(
					className);

		}
		String tableName = NameGenerator.getTableName(clazz, adapter);
		if (!tableManager.tableExists(tableName, cw))
		{
			return res;
		}

		StatementPrototypeGenerator whereGenerator = new StatementPrototypeGenerator(
				adapter);
		whereGenerator.setClauses(clause);
		StatementPrototype sp = whereGenerator.generate(clazz, true);
		// get the id of clazz
		String shortName = whereGenerator.getTypeStack()
				.getActualRepresentation().getAsName();

		StringBuilder statement = new StringBuilder("SELECT ");
		statement.append(shortName);
		statement.append(".");
		statement.append(Defaults.ID_COL);
		statement.append(",");
		statement.append(shortName);
		statement.append(".");
		statement.append(Defaults.REAL_CLASS_COL);
		statement.append(",");
		statement.append(shortName);
		statement.append(".");
		statement.append(Defaults.REAL_ID_COL);
		statement.append(" FROM ");
		if (id != null)
		{
			sp.addEqualsClause(shortName + "." + Defaults.ID_COL, id);
		}
		PreparedStatement ps = sp.toPreparedStatement(cw, statement.toString());
		ResultSet rs = ps.executeQuery();
		List<HashMap<String, Object>> propertyVector = createPropertyVector(rs);
		ps.close();
		// If a row has a REALCLASS entry, load data for the subclass
		for (HashMap<String, Object> map : propertyVector)
		{
			Long dbId = ((Number) map.get(Defaults.ID_COL)).longValue();

			// If a row has a REALCLASS entry, load the subclass
			if (map.get(Defaults.REAL_CLASS_COL) != null)
			{
				// get the real class and id
				className = (String) map.get(Defaults.REAL_CLASS_COL);
				dbId = ((Number) map.get(Defaults.REAL_ID_COL)).longValue();
				// get the data for the real class
				HashMap<Class<?>, ArrayList<Long>> tmpRes = getObjectDescriptors(
						null, className, null, dbId, cw);
				for(Entry<Class<?>, ArrayList<Long>> en:tmpRes.entrySet())
				{
					Class<?>c = en.getKey();
					ArrayList<Long> existingList = res.get(c);
					if (existingList == null)
					{
						res.putAll(tmpRes);
					}
					else
					{
						existingList.addAll(en.getValue());
					}
				}
			}
			else
			{
				// no REAL_CLASS_COL found, which means this is the actual class
				// of the object
				ArrayList<Long> idVector = res.get(clazz);
				if (idVector == null)
				{
					idVector = new ArrayList<Long>();
					res.put(clazz, idVector);
				}
				idVector.add(dbId);
			}
		}
		return res;

	}

	/**
	 * Get the last id/unique id inserted into the named table by this
	 * connection
	 * 
	 * @throws SQLException
	 */
	public long getLastId(ConnectionWrapper cw, String tableName)
			throws SQLException
	{
		long res = 0;
		PreparedStatement ps = cw.prepareStatement(adapter
				.getLastInsertedIdentity(tableName));
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();
		if (rs.next())
		{
			res = rs.getLong(1);
		}
		ps.close();
		return res;
	}

	/**
	 * Add an object to the database. If the object already exists, it will be
	 * updated. Convenience method that does not require the user to supply a
	 * ConnectionWrapper.
	 * 
	 * @param object
	 *            the object to save.
	 * 
	 * @throws SQLException
	 */
	void saveObject(Object object) throws SQLException
	{
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			saveObject(cw, object, true, null);
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
	 * Add an object to the database. If the object already exists, it will be
	 * updated.
	 * 
	 * @param object
	 *            the object to save.
	 * @throws IOException
	 * @throws SQLException
	 * 
	 * @throws SQLException
	 */
	public void saveObjectUnprotected(ConnectionWrapper cw, Object object)
			throws SQLException, IOException
	{
		saveObject(cw, object, false, null);
	}

	/**
	 * Save the object without creating an external protection entry.
	 * 
	 * @param object
	 *            the object to save.
	 * @param cw
	 *            the connection wrapper used for this transaction.
	 * @return the database id of the saved object, or null if the object was
	 *         not saved.
	 * @throws SQLException
	 * @throws IOException
	 */
	public Long saveObjectUnprotected(ConnectionWrapper cw, Object object,
			DelayedInsertionBuffer delayBuffer) throws SQLException
	{
		return saveObject(cw, object, false, delayBuffer);
	}

	public Long saveObject(ConnectionWrapper cw, Object object, boolean protect,
			DelayedInsertionBuffer delayBuffer) throws SQLException
	{
		Long id = (long) System.identityHashCode(object);
		if (delayBuffer == null)
		{
			delayBuffer = new DelayedInsertionBuffer(adapter.getPersist());
		}
		else if (delayBuffer.isKnown(id))
		{
			Long dbId = cache.getDatabaseId(object);
			return dbId;
		}
		delayBuffer.addId(id);
		Long res = null;
		String tableName = null;
		if (object.getClass().isArray())
		{
			tableName = Defaults.ARRAY_TABLE_NAME;
		}
		else
		{
			tableName = NameGenerator.getTableName(object, adapter);
		}
		// check if the object exists
		Long databaseId = cache.getDatabaseId(object);
		if (databaseId == null)
		{
			// the object is unknown
			ObjectStack stack = new ObjectStack(this.adapter,
					object.getClass(), object, delayBuffer);
			stack.save(cw);
			res = stack.getActualRepresentation().getId();
			// label the object as having been inserted by the outside
			if (protect)
			{
				protectionManager.protectObjectExternal(tableName, res,
						ObjectTools.getSystemicName(object.getClass()), cw);
			}
		}
		else
		{
			res = databaseId;
			if (protect
					&& !protectionManager.isProtectedExternal(tableName,
							databaseId, cw))
			{
				protectionManager.protectObjectExternal(tableName, databaseId,
						ObjectTools.getSystemicName(object.getClass()), cw);
			}
			updater.updateObject(cw, object, tableName, databaseId, delayBuffer);
		}
		// insert the objects in the delay buffer
		delayBuffer.insertObjects(cw, protectionManager);
		return res;
	}

	/**
	 * Return a list of objects of a given class (including subclasses and/or
	 * implementing classes) that satisfy the given clause. Convenience method
	 * that does not require the user to supply a ConnectionWrapper.
	 * 
	 * @param <T>
	 *            the type of objects to return
	 * @param clazz
	 *            the class of objects to return, subclasses will also be
	 *            returned.
	 * @param clause
	 *            the clause that all the returned objects must satisfy.
	 * @return an ArrayList of the desired type.
	 * @throws SQLException
	 */
	public <T> List<T> getObjects(Class<T> clazz, Clause... clause)
			throws SQLException
	{
		List<T> res = null;
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			res = getObjects(cw, clazz, clause);
			cw.commitAndDiscard();
		}
		catch (Exception e)
		{
			cw.rollbackAndDiscard();
			throw new SQLException(e);
		}
		return res;
	}

	/**
	 * Return a list of objects of a given class (including subclasses and/or
	 * implementing classes) that satisfy the given clause.
	 * 
	 * @param <T>
	 *            the type of objects to return
	 * @param clazz
	 *            the class of objects to return, subclasses will also be
	 *            returned.
	 * @param clause
	 *            the clause that all the returned objects must satisfy.
	 * @param cw
	 *            the connection wrapper to use for this operation.
	 * @return an ArrayList of the desired type.
	 * @throws SQLException
	 */
	public <T> List<T> getObjects(ConnectionWrapper cw, Class<T> clazz,
			Clause... clause) throws SQLException
	{
		ArrayList<T> res = new ArrayList<T>();
		try
		{
			if (!clazz.isInterface())
			{
				// get an actual object from the database
				res.addAll(getObjectsNonInterface(clazz, cw, clause));
			}
			else
			{
				// get all implementing classes, too
				List<Class<? extends T>> implementingClasses = getImplementingClasses(
						clazz, cw);
				removeDuplicateSubClasses(implementingClasses);
				for (Class<? extends T> implementingClass : implementingClasses)
				{
					// recurse for all subclasses
					res.addAll(getObjects(cw, implementingClass, clause));
				}
			}
		}
		catch (ClassNotFoundException e)
		{
			throw new SQLException(e);
		}
		return res;
	}

	/**
	 * Get the number of database objects of class clazz that satisfy the
	 * clause. Convenience method that does not require the user to supply a
	 * ConnectionWrapper.
	 * 
	 * @param <T>
	 * @param clazz
	 *            the class to look for.
	 * @param clause
	 *            the clause that must be satisfied.
	 * @return the number of objects of class clazz and its subclasses that
	 *         satisfy clause.
	 * @throws SQLException
	 */
	<T> long getCount(Class<T> clazz, Clause clause) throws SQLException
	{
		long res = 0;
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			res = getCount(cw, clazz, clause);
		}
		catch (Exception e)
		{
			cw.rollback();
			throw new SQLException(e);
		}
		cw.discard();
		return res;
	}

	/**
	 * Get the number of database objects of class clazz that satisfy the
	 * clause.
	 * 
	 * @param <T>
	 * @param clazz
	 * @param clause
	 *            the clause that must be satisfied.
	 * @param cw
	 *            the connection wrapper for this operation.
	 * @return the number of objects of class clazz and its subclasses that
	 *         satisfy clause.
	 * @throws SQLException
	 */
	<T> long getCount(ConnectionWrapper cw, Class<T> clazz, Clause clause)
			throws SQLException
	{
		long res = 0;
		try
		{
			if (!clazz.isInterface())
			{
				// get an actual object from the database
				res += getCountNonInterface(cw, clazz, clause);
			}
			else
			{
				// get all implementing classes, too
				List<Class<? extends T>> subClasses = getImplementingClasses(
						clazz, cw);
				removeDuplicateSubClasses(subClasses);
				for (Class<? extends T> subClass : subClasses)
				{
					// recurse for all subclasses
					res += getCount(subClass, clause);
				}
			}
		}
		catch (ClassNotFoundException e)
		{
			throw new SQLException(e);
		}
		return res;
	}

	/**
	 * Remove the entries from classes that is the subclass of any other class
	 * in the list.
	 * 
	 * @param subClasses
	 *            the List to check for subclasses.
	 */
	private <T> void removeDuplicateSubClasses(
			List<Class<? extends T>> subClasses)
	{
		for (int x = 0; x < subClasses.size(); x++)
		{
			// get the class at this location
			Class<? extends T> clazz = subClasses.get(x);
			// check if the class is a subclass
			for (int y = 0; y < subClasses.size(); y++)
			{
				if (x != y)
				{
					Class<? extends T> possibleSubClass = subClasses.get(y);
					if (isSubClassOf(clazz, possibleSubClass))
					{
						// remove the subclass
						subClasses.remove(y);
						// take one step back
						y--;
						if (y < x)
						{
							// the list has changed before the position of x,
							// step out
							x = y;
							break;
						}
					}
				}
			}
		}
	}

	/**
	 * Checks if possibleSubClass is in the inheritance tree below clazz.
	 * 
	 * @param clazz
	 *            the possible parent class.
	 * @param possibleSubClass
	 *            the class to check for dependence on clazz.
	 * @return true if possibleSubClass is a subclass of clazz.
	 */
	private boolean isSubClassOf(Class<?> clazz, Class<?> possibleSubClass)
	{
		Class<?> superClass = possibleSubClass.getSuperclass();
		while (superClass != null)
		{
			if (superClass.equals(clazz))
			{
				return true;
			}
			superClass = superClass.getSuperclass();
		}
		return false;
	}

	/**
	 * Find all classes that implements or extends clazz.
	 * 
	 * @param <T>
	 * @param clazz
	 *            the class (usually an interface) that we want to list the
	 *            subclasses for.
	 * @return a list of classes that implement or extend the interface or class
	 *         represented by clazz.
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	private <T> List<Class<? extends T>> getImplementingClasses(Class<T> clazz,
			ConnectionWrapper cw) throws SQLException, ClassNotFoundException
	{
		ArrayList<Class<? extends T>> res = new ArrayList<Class<? extends T>>();
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT SUBCLASS FROM ");
		sb.append(Defaults.IS_A_TABLENAME);
		sb.append(" WHERE SUPERCLASS = ?");
		PreparedStatement ps = cw.prepareStatement(sb.toString());
		ps.setString(1, ObjectTools.getSystemicName(clazz));
		Tools.logFine(ps);
		ResultSet rs = ps.executeQuery();

		while (rs.next())
		{
			String subClassName = rs.getString(1);
			Class<? extends T> c = (Class<? extends T>) ClassLoader
					.getSystemClassLoader().loadClass(subClassName);
			if (c.isInterface())
			{
				res.addAll(getImplementingClasses(c, cw));
			}
			else
			{
				res.add(c);
			}
		}
		ps.close();
		return res;
	}

	public Object checkCache(String tableName, Long id)
	{
		return this.cache.getObject(tableName, id);
	}

	public void saveToCache(String tableName, Object o, Long dbId)
	{
		this.cache.storeObject(tableName, o, dbId);
	}

	/**
	 * Get all objects of class clazz that satisfy the clause.
	 * 
	 * @param clazz
	 * @param className
	 * @param clauses
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	private <T> List<T> getObjectsNonInterface(Class<T> clazz,
			ConnectionWrapper cw, Clause... clauses) throws SQLException,
			ClassNotFoundException
	{
		for (Clause clause : clauses)
		{
			clause.setQueryClass(clazz);
		}
		ArrayList<T> res = new ArrayList<T>();
		if (!tableManager.tableExists(clazz, cw))
		{
			return res;
		}

		StatementPrototypeGenerator whereGenerator = new StatementPrototypeGenerator(
				adapter);
		whereGenerator.setClauses(clauses);
		StatementPrototype sp = whereGenerator.generate(clazz, true);
		PreparedStatement ps = sp.toPreparedStatement(cw,
				sp.getSelectStartQuery());
		ResultSet rs = ps.executeQuery();
		List<HashMap<String, Object>> propertyVector = createPropertyVector(rs);
		ps.close();
		for (HashMap<String, Object> map : propertyVector)
		{
			Long dbId = ((Number) map.get(Defaults.ID_COL)).longValue();
			// If a row has a REALCLASS entry, load the subclass
			if (map.get(Defaults.REAL_CLASS_COL) != null)
			{
				// get the real class and id
				dbId = ((Number) map.get(Defaults.REAL_ID_COL)).longValue();
				String className = (String) map.get(Defaults.REAL_CLASS_COL);
				if (className.equals(Defaults.ARRAY_TABLE_NAME))
				{
					// arrays are not loaded in response to WHERE queries,
					// only as members of specific objects.
					continue;
				}
				else
				{
					// load the real class
					clazz = (Class<T>) ClassLoader.getSystemClassLoader()
							.loadClass(className);
					// primitives are not loaded in response to queries, only as
					// parts of other objects
					if (!ObjectTools.isPrimitive(clazz)
							&& !(clazz.equals(MapEntry.class))
							&& !(clazz.equals(Number.class)))
					{
						// get the subclass-specific data
						getSubClassData(map, clazz, dbId, cw);
						// load the real class info
						className = (String) map.get(Defaults.REAL_CLASS_COL);
						dbId = ((Number) map.get(Defaults.ID_COL)).longValue();
						clazz = (Class<T>) ClassLoader.getSystemClassLoader()
								.loadClass(className);

					}
					else
					{
						continue;
					}
				}
			}
			if (!clazz.isArray())
			{
				String tableName = NameGenerator.getTableName(clazz, adapter);
				// check if the object is known
				Object cachedObject = cache.getObject(tableName, dbId);
				if (cachedObject == null)
				{
					// object was not found in cache
					// create new object
					T nuObject = ObjectFactory.createObject(adapter, cache,
							map, clazz, cw, tableName, dbId);
					res.add(nuObject);
					// add object to cache
					cache.storeObject(tableName, nuObject, dbId);
				}
				else
				{
					// the object was found, add it to result array
					res.add((T) cachedObject);
				}
			}
		}
		return res;
	}

	/**
	 * Get the number of objects of class clazz that satisfy the clause.
	 * 
	 * @param clazz
	 * @param className
	 * @param clause
	 * @return the number of matching objects.
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	private <T> long getCountNonInterface(ConnectionWrapper cw, Class<T> clazz,
			Clause... clause) throws SQLException
	{
		long res = 0;
		if (!tableManager.tableExists(clazz, cw))
		{
			return res;
		}

		StatementPrototypeGenerator whereGenerator = new StatementPrototypeGenerator(
				adapter);
		whereGenerator.setClauses(clause);
		StatementPrototype sp = whereGenerator.generate(clazz, true);
		String tableId = sp.getIdStatementGenerator().getJoinTableIds().get(0);
		StringBuilder statement = new StringBuilder("SELECT COUNT(DISTINCT("
				+ tableId + "." + Defaults.ID_COL + ")) FROM ");
		PreparedStatement ps = sp.toPreparedStatement(cw, statement.toString());
		ResultSet rs = ps.executeQuery();
		if (rs.next())
		{
			res = rs.getLong(1);
		}
		ps.close();
		return res;
	}

	/**
	 * Get the object of class clazz with the given database id. The actual
	 * object returned may be an instance of a subclass.
	 * 
	 * This method does not create a protection entry.
	 * 
	 * @param clazz
	 *            the class of the object to retrieve.
	 * @param id
	 *            the database id of the object at the level of clazz.
	 * @param cw
	 *            the connection wrapper to use for this operation.
	 * @return the matching object.
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public <T> T getObject(ConnectionWrapper cw, Class<T> clazz, Long id)
			throws SQLException, ClassNotFoundException
	{
		return getObject(cw, clazz, id, this.cache);
	}

	@SuppressWarnings("unchecked")
	public <T> T getObject(ConnectionWrapper cw, Class<T> clazz, Long id,
			ObjectRowMap cache) throws SQLException, ClassNotFoundException
	{
		if (clazz.isInterface())
		{
			clazz = (Class<T>) Object.class;
		}
		String className = ObjectTools.getSystemicName(clazz);
		T res = null;
		if (!tableManager.tableExists(
				NameGenerator.getTableName(clazz, adapter), cw))
		{
			return res;
		}

		StatementPrototypeGenerator whereGenerator = new StatementPrototypeGenerator(
				adapter);
		StatementPrototype sp = whereGenerator.generate(clazz, true);
		String shortName = whereGenerator.getTypeStack()
				.getRepresentation(clazz).getAsName();

		sp.addEqualsClause(shortName + "." + Defaults.ID_COL, id);

		PreparedStatement ps = sp.toPreparedStatement(cw,
				sp.getSelectStartQuery());
		ResultSet rs = ps.executeQuery();
		List<HashMap<String, Object>> propertyVector = createPropertyVector(rs);
		ps.close();
		for (HashMap<String, Object> map : propertyVector)
		{
			Long dbId = ((Number) map.get(Defaults.ID_COL)).longValue();
			// If a row has a REALCLASS entry, load the subclass
			if (map.get(Defaults.REAL_CLASS_COL) != null)
			{
				dbId = ((Number) map.get(Defaults.REAL_ID_COL)).longValue();
				// get the real class and id
				className = (String) map.get(Defaults.REAL_CLASS_COL);
				if (!className.equals(Defaults.ARRAY_TABLE_NAME))
				{
					// load the real class
					clazz = (Class<T>) ClassLoader.getSystemClassLoader()
							.loadClass(className);
					// get the subclass-specific data
					getSubClassData(map, clazz, dbId, cw);
					// load the real class info
					className = (String) map.get(Defaults.REAL_CLASS_COL);
					dbId = ((Number) map.get(Defaults.ID_COL)).longValue();
				}
			}

			// check if the object is an array
			if (clazz.isArray() || className.equals(Defaults.ARRAY_TABLE_NAME))
			{

				// the referenced object is an array, so load it
				Object cachedObject = cache.getObject(
						Defaults.ARRAY_TABLE_NAME, dbId);

				if (cachedObject == null)
				{
					// object was not found in cache
					// create new array object
					ArrayLoader arrayLoader = new ArrayLoader(this.adapter,
							cache, cw);
					arrayLoader.loadArray(dbId);
					T nuObject = (T) arrayLoader.getArray();
					res = nuObject;
				}
				else
				{
					// the object was found, add it to result array
					res = (T) cachedObject;
				}
			}
			else
			{
				// load an ordinary object
				// check if the object is known
				clazz = (Class<T>) ClassLoader.getSystemClassLoader()
						.loadClass(className);
				String tableName = NameGenerator.getTableName(clazz, adapter);
				Object cachedObject = cache.getObject(tableName, dbId);
				if (cachedObject == null)
				{
					// object was not found in cache
					// create new object
					res = ObjectFactory.createObject(adapter, cache, map,
							clazz, cw, tableName, dbId);
				}
				else
				{
					// the object was found, add it to result array
					res = (T) cachedObject;
				}
			}
		}
		return res;
	}

	/**
	 * Get a list of all classes persisted in this database. It does not include
	 * classes representing primitives, e.g. java.lang.Integer, or array
	 * classes.
	 * 
	 * @return a list of classes.
	 * @throws SQLException
	 */
	public List<Class<?>> getClasses() throws SQLException
	{
		ArrayList<Class<?>> res = new ArrayList<Class<?>>();
		// get all c__IS_A entries
		String selectStmt = "SELECT * from " + Defaults.IS_A_TABLENAME;
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			PreparedStatement ps = cw.prepareStatement(selectStmt);
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				String superClassName = rs.getString("SUPERCLASS");
				String subClassName = rs.getString("SUBCLASS");
				try
				{
					Class<?> superClass = ObjectTools
							.lookUpClass(superClassName);
					if (superClass != null && !res.contains(superClass))
					{
						res.add(superClass);
					}
				}
				catch (ClassNotFoundException e)
				{
					LOGGER.log(Level.WARNING, "ClassNotFoundException: ", e);
				}
				try
				{
					Class<?> subClass = ObjectTools.lookUpClass(subClassName);
					if (subClass != null && !res.contains(subClass))
					{
						res.add(subClass);
					}
				}
				catch (ClassNotFoundException e)
				{
					LOGGER.log(Level.WARNING, "ClassNotFoundException: ", e);
				}

			}
			ps.close();
			cw.commitAndDiscard();
		}
		catch (Exception e)
		{
			cw.rollbackAndDiscard();
			throw new SQLException(e);
		}

		return res;
	}

	/**
	 * Recursively descend the inheritance tree, loading all properties.
	 * 
	 * @param <T>
	 * @param map
	 *            a property-value map of the object to get
	 * @param clazz
	 *            the class to look up values for.
	 * @param dbId
	 *            the unique identifier in the database table.
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	private <T> void getSubClassData(HashMap<String, Object> map,
			Class<T> clazz, Long dbId, ConnectionWrapper cw)
			throws ClassNotFoundException, SQLException
	{
		String realClassName = (String) map.get(Defaults.REAL_CLASS_COL);
		// erase the 'real' class entry
		map.remove(Defaults.REAL_CLASS_COL);
		StringBuilder statement = new StringBuilder("SELECT * FROM ");
		StatementPrototypeGenerator whereGenerator = new StatementPrototypeGenerator(
				adapter);
		StatementPrototype sp = whereGenerator.generate(clazz, false);
		sp.addEqualsClause(Defaults.ID_COL, dbId);
		PreparedStatement ps = sp.toPreparedStatement(cw, statement.toString());
		ResultSet rs = ps.executeQuery();
		List<HashMap<String, Object>> propertyVector = createPropertyVector(rs);
		if (propertyVector.size() != 1)
		{
			throw new SQLException("Wrong number of subclass entities found: "
					+ propertyVector.size() + ", expected 1.");
		}
		ps.close();
		HashMap<String, Object> subMap = propertyVector.get(0);
		// merge the two maps
		map.putAll(subMap);
		// check if we need to recurse down the class hierarchy
		String subClassName = (String) map.get(Defaults.REAL_CLASS_COL);
		if (subClassName != null)
		{
			Class<?> subClass = (Class<T>) ClassLoader.getSystemClassLoader()
					.loadClass(subClassName);
			getSubClassData(map, subClass,
					((Number) map.get(Defaults.REAL_ID_COL)).longValue(), cw);
		}
		else
		{
			// otherwise, put the real class name back
			map.put(Defaults.REAL_CLASS_COL, realClassName);
		}
	}

	/**
	 * Create a mapping from table/member names to values, based on the rows
	 * returned from the ResultSet.
	 * 
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	private List<HashMap<String, Object>> createPropertyVector(ResultSet rs)
			throws SQLException
	{
		ArrayList<HashMap<String, Object>> res = new ArrayList<HashMap<String, Object>>();
		whileLoop: while (rs.next())
		{
			HashMap<String, Object> map = new HashMap<String, Object>();
			fillValues(rs, map);
			if (!adapter.handlesDistinctWithClobsAndBlobsCorrectly())
			{
				// the DB engine we are using does not handle DISTINCT(...) in
				// conjuction with Clobs and/or Blobs, so we are forced to
				// manually check that we don't insert double entries.
				Long nuId = (Long) map.get(Defaults.ID_COL);
				for (HashMap<String, Object> tmpMap : res)
				{
					Long existingId = (Long) tmpMap.get(Defaults.ID_COL);
					if (nuId.equals(existingId))
					{
						// do not insert the new map, as it currently already
						// exists.
						continue whileLoop;
					}
				}
				// if we got this far without finding a duplicate map, insert
				// the new map
				res.add(map);

			}
			else
			{
				res.add(map);
			}
		}
		return res;
	}

	/**
	 * Fill the set values of the result set into the HashMap. The ResultSet
	 * will not be advanced beyond its current row.
	 * 
	 * @param rs
	 * @param map
	 * @throws SQLException
	 */
	private void fillValues(ResultSet rs, HashMap<String, Object> map)
			throws SQLException
	{

		ResultSetMetaData md = rs.getMetaData();
		for (int x = 0; x < md.getColumnCount(); x++)
		{
			// for all the column names, set the corresponding property
			String key = md.getColumnName(x + 1);
			if (adapter.tableNamesAreLowerCase())
			{
				key = key.toUpperCase();
			}
			if (map.get(key) == null)
			{
				map.put(key, rs.getObject(x + 1));
			}
		}
	}

	void close()
	{
		if (adapter.getShutdownCommand() != null)
		{
			ConnectionWrapper cw = null;
			try
			{
				cw = getConnectionWrapper();
				PreparedStatement ps = cw.prepareStatement(adapter
						.getShutdownCommand());
				ps.execute();
				ps.close();
				cw.commitAndDiscard();
			}
			catch (Exception e)
			{
				LOGGER.log(Level.WARNING, "Exception: ", e);
				try
				{
					if (cw != null)
					{
						cw.rollbackAndDiscard();
					}
				}
				catch (SQLException e1)
				{
					LOGGER.log(Level.WARNING, "SQLException: ", e1);
				}
			}
		}
		connectionPool.cleanUp();
		if (adapter.getDriverManagerShutdownCommand() != null)
		{
			try
			{
				Connection c = DriverManager.getConnection(adapter
						.getDriverManagerShutdownCommand());
				if (c != null)
				{
					c.close();
				}
			}
			catch (SQLNonTransientConnectionException e)
			{
				// ignored
			}
			catch (SQLException e)
			{
				LOGGER.log(Level.WARNING, "SQLException: ", e);
			}
		}
		cache.stop();
	}

	/**
	 * Get the db id of object of class realClass in the table of class c.
	 * 
	 * @param c
	 *            the class to find the id in.
	 * @param realClass
	 *            the actual class of the object
	 * @param id
	 *            the id in the table of the actual class
	 * @return the id for the object of class realClass with the given id, cast
	 *         to class c.
	 * @throws SQLException
	 */
	public Long getCastId(Class<?> c, Class<?> realClass, long id,
			ConnectionWrapper cw) throws SQLException
	{
		if (c.equals(realClass))
		{
			// no need to change it, as it's already cast to the correct class.
			return id;
		}
		else
		{
			StatementPrototypeGenerator gen = new StatementPrototypeGenerator(
					adapter);

			StatementPrototype sp = gen.generate(realClass, true);
			sp.addEqualsClause(
					gen.getTypeStack()
							.getRepresentation(
									gen.getTypeStack().getLevel(realClass))
							.getAsName()
							+ "." + Defaults.ID_COL, id);
			StringBuilder prepend = new StringBuilder("SELECT ");
			prepend.append(gen.getTypeStack()
					.getRepresentation(gen.getTypeStack().getLevel(c))
					.getAsName());
			prepend.append(".");
			prepend.append(Defaults.ID_COL);
			prepend.append(" FROM ");
			PreparedStatement ps = sp.toPreparedStatement(cw,
					prepend.toString());
			try
			{
				Tools.logFine(ps);
				ResultSet rs = ps.executeQuery();
				if (rs.next())
				{
					return rs.getLong(1);
				}
				else
				{
					return null;
				}
			}
			finally
			{
				ps.close();
			}
		}
	}

	public AdapterBase getAdapter()
	{
		return this.adapter;
	}

	public TableManager getTableManager()
	{
		return this.tableManager;
	}

	public ProtectionManager getProtectionManager()
	{
		return this.protectionManager;
	}

	/**
	 * Get the id of the given object.
	 * 
	 * @param o
	 *            the object to get the id for.
	 * @return the database id of component, if it is cached.
	 */
	public Long getId(Object o)
	{
		return cache.getDatabaseId(o);
	}

	public ArrayEntryWriter getArrayEntryWriter()
	{
		return arrayEntryWriter;
	}

	public String getConnectionString()
	{
		return connectionString;
	}

	/**
	 * Recursively descend a dependency tree until the bottom is reached. This
	 * method looks for the ultimate subclass of an object and returns its name
	 * and the corresponding id.
	 * 
	 * @param cw
	 *            the ConnectionWrapper to use - will be passed on recursively.
	 * @param propertyClass
	 *            the class to look for subclasses of.
	 * @param propertyId
	 *            the database id corresponding to propertyClass.
	 * @return the name of the table describing the ultimate subclass, and the
	 *         id value for that table.
	 * @throws SQLException
	 */
	public ClassIdTuple getRealTableNameAndId(ConnectionWrapper cw,
			Class<?> propertyClass, Long propertyId) throws SQLException
	{
		ClassIdTuple res = null;
		String propertyTable = NameGenerator.getTableName(propertyClass,
				adapter);
		StringBuilder query = new StringBuilder("SELECT ");
		query.append(Defaults.REAL_CLASS_COL);
		query.append(",");
		query.append(Defaults.REAL_ID_COL);
		query.append(" FROM ");
		query.append(propertyTable);
		query.append(" WHERE ");
		query.append(Defaults.ID_COL);
		query.append("=?");
		PreparedStatement ps = cw.prepareStatement(query.toString());
		ps.setLong(1, propertyId);
		ResultSet rs = ps.executeQuery();
		if (rs.next())
		{
			String realName = rs.getString(1);
			if (realName == null)
			{
				res = new ClassIdTuple(propertyClass, propertyId);
			}
			else
			{
				Long realId = rs.getLong(2);
				try
				{
					// convert to table name
					Class<?> realClass = ObjectTools.lookUpClass(realName);
					res = getRealTableNameAndId(cw, realClass, realId);
				}
				catch (ClassNotFoundException e)
				{
					throw new SQLException(e);
				}
			}

		}
		else
		{
			ps.close();
			throw new SQLException("Could not find entry in " + propertyTable
					+ " with id " + propertyId);
		}
		ps.close();
		return res;
	}

	/**
	 * Refresh an object. Get all the data in the object from the database,
	 * ignoring the cache.
	 * 
	 * Objects that were, but are no longer, part of obj are not updated.
	 * 
	 * @param obj
	 *            the object to refresh.
	 * @param cw
	 *            the connection wrapper.
	 * @return the refreshed object or null if the object is no longer in the
	 *         database.
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	<T> T refresh(ConnectionWrapper cw, T obj) throws SQLException
	{

		try
		{
			T res = obj;
			Long dbId = cache.getDatabaseId(obj);
			ObjectRowMap tmpCache = new ObjectRowMap();
			tmpCache.start();
			Object nuObject = this
					.getObject(cw, obj.getClass(), dbId, tmpCache);
			if (nuObject == null)
			{
				res = null;
			}
			else
			{
				ObjectRepresentation orig = new ObjectRepresentation(adapter,
						obj.getClass(), obj, null);
				ObjectRepresentation nu = new ObjectRepresentation(adapter,
						nuObject.getClass(), nuObject, null);
				ArrayList<Long> idList = new ArrayList<Long>();
				refresh(orig, cache, nu, tmpCache, idList);
			}
			return res;
		}
		catch (IllegalAccessException e)
		{
			throw new SQLException(e);
		}
		catch (InvocationTargetException e)
		{
			throw new SQLException(e);
		}
		catch (ClassNotFoundException e)
		{
			throw new SQLException(e);
		}
	}

	private void refresh(ObjectRepresentation orig, ObjectRowMap origCache,
			ObjectRepresentation nu, ObjectRowMap nuCache,
			ArrayList<Long> idList) throws IllegalArgumentException,
			IllegalAccessException, InvocationTargetException
	{
		for (int x = 0; x < orig.getPropertyCount(); x++)
		{
			Object origProperty = orig.getPropertyValue(x);
			Object nuProperty = nu.getPropertyValue(x);
			if (origProperty == null)
			{
				if (nuProperty != null)
				{
					orig.setPropertyValue(x, nuProperty);
					origCache.storeObject(orig.getTableName(), nuProperty,
							nuCache.getDatabaseId(nuProperty));
				}
			}
			else if (nuProperty == null)
			{
				// property has been deleted, remove reference
				orig.setPropertyValue(x, null);
			}
			else
			{
				// both origProperty and nuProperty are non-null
				if (orig.isPrimitive(x))
				{
					if (origProperty.equals(nuProperty))
					{
						// the id, and therefore the objects, are unchanged.
						// no point in recursing into primitives
					}
					else
					{
						orig.setPropertyValue(x, nuProperty);
					}
				}
				else
				{
					Long origDbId = origCache.getDatabaseId(origProperty);
					Long nuDbId = nuCache.getDatabaseId(nuProperty);
					if (origDbId.equals(nuDbId))
					{
						// the id, and therefore the objects, are unchanged.
						// recurse
						ObjectRepresentation origPropertyPresentation = new ObjectRepresentation(
								adapter, origProperty.getClass(), origProperty,
								null);
						ObjectRepresentation nuPropertyPresentation = new ObjectRepresentation(
								adapter, nuProperty.getClass(), nuProperty,
								null);
						if (!idList.contains(origDbId))
						{
							idList.add(origDbId);
							refresh(origPropertyPresentation, origCache,
									nuPropertyPresentation, nuCache, idList);
						}
					}
					else
					{
						orig.setPropertyValue(x, nuProperty);
						origCache.storeObject(orig.getTableName(), nuProperty,
								nuDbId);
					}
				}
			}
		}
	}

	/**
	 * Clone this database into another database.
	 * 
	 * After this method completes successfully, the target database will
	 * contain an exact copy of this database. Schema and contents will be
	 * copied, but there is no guarantee that the C__ID fields will be the same.
	 * If this database or the target database is written to during this
	 * operation, the outcome is undefined.
	 * 
	 * If this operation fails the state of the target database is undefined.
	 * 
	 * This operation does not interfere with existing objects in the target
	 * database.
	 * 
	 * This operation does not change the state of this database.
	 * 
	 * @param target
	 *            the target database.
	 * @throws SQLException
	 * 
	 */
	void duplicate(Persist target) throws SQLException
	{
		// basic sanity check
		if (this.equals(target))
		{
			throw new IllegalArgumentException(
					"Can not copy database into itself.");
		}
		Duplicator dup = new Duplicator(this, target);
		dup.doCopy();
	}

	public ObjectRowMap getCache()
	{
		return this.cache;
	}

}
