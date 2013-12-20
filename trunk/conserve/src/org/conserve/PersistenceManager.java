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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.conserve.connection.ConnectionWrapper;
import org.conserve.select.All;
import org.conserve.select.Clause;
import org.conserve.select.discriminators.Equal;

/**
 * Object database interface. Saves to and retrieves from a persistence
 * database.
 * 
 * This is the main programming interface class. Create an instance of this
 * class to interact with the database.
 * 
 * 
 * 
 * @author Erik Berglund
 * 
 */
public class PersistenceManager
{
	private Persist persist;

	/**
	 * Load the settings from a file. The file contains properties on the form
	 * property=value
	 * 
	 * @param filename
	 *            the name of the properties file to load settings from.
	 * @throws IOException
	 * @throws SQLException
	 */
	public PersistenceManager(String filename) throws IOException, SQLException
	{
		this(filename, true);
	}

	/**
	 * Load the settings from a file. The file contains properties on the form
	 * property=value
	 * 
	 * If createSchema is true the database tables will be automatically
	 * created.
	 * 
	 * @param filename
	 *            the name of the properties file to load settings from.
	 * @param createSchema
	 *            whether to create tables or not.
	 * @throws IOException
	 * @throws SQLException
	 */
	public PersistenceManager(String filename, boolean createSchema) throws IOException, SQLException
	{
		Properties p = new Properties();
		InputStream in = ClassLoader.getSystemClassLoader().getResourceAsStream(filename);
		if (in == null)
		{
			File f = new File(filename);
			in = new FileInputStream(f);
		}
		p.load(in);
		setup(p, createSchema);
		in.close();
	}

	/**
	 * Reads property=value pairs from the inputstream.
	 * 
	 * @param in
	 *            the source of the properties of the connection.
	 * @throws IOException
	 * @throws SQLException
	 */
	public PersistenceManager(InputStream in) throws IOException, SQLException
	{
		this(in, true);
	}

	/**
	 * Reads property=value pairs from the inputstream. If createSchema is true
	 * the database tables will be automatically created.
	 * 
	 * @param in
	 *            the source of the properties of the connection.
	 * @param createSchema
	 *            whether to create tables or not.
	 * @throws IOException
	 * @throws SQLException
	 */
	public PersistenceManager(InputStream in, boolean createSchema) throws IOException, SQLException
	{
		Properties prop = new Properties();
		prop.load(in);
		setup(prop, createSchema);
	}

	/**
	 * Create object, read settings from provided properties.
	 * 
	 * @param prop
	 *            contains the driver, connectionstring, username and password
	 *            strings.
	 * @throws SQLException
	 */
	public PersistenceManager(Properties prop) throws SQLException
	{
		this(prop, true);
	}

	/**
	 * Create object, read settings from provided properties.
	 * 
	 * If createSchema is true the database tables will be automatically
	 * created.
	 * 
	 * @param createSchema
	 *            whether to create tables or not.
	 * @param prop
	 *            contains the driver, connectionstring, username and password
	 *            strings.
	 * @throws SQLException
	 */
	public PersistenceManager(Properties prop, boolean createSchema) throws SQLException
	{
		setup(prop, createSchema);
	}

	private void setup(Properties prop, boolean createSchema) throws SQLException
	{
		persist = new Persist();
		persist.setCreateSchema(createSchema);
		persist.initialize(prop);
	}

	/**
	 * @param driver
	 *            the driver name, optionally null if JDBC version is 4 or
	 *            greater.
	 * @param connectionstring
	 *            the connection string to the database.
	 * @param username
	 *            the database username.
	 * @param password
	 *            the database password.
	 * @throws SQLException
	 */
	public PersistenceManager(String driver, String connectionstring, String username, String password)
			throws SQLException
	{
		this(driver, connectionstring, username, password, true);
	}

	/**
	 * 
	 * @param driver
	 *            the driver name, optionally null if JDBC version is 4 or
	 *            greater.
	 * @param connectionstring
	 *            the connection string.
	 * @param username
	 *            the database username.
	 * @param password
	 *            the database password.
	 * @param createSchema
	 *            whether to create database tables or not.
	 * @throws SQLException
	 */
	public PersistenceManager(String driver, String connectionstring, String username, String password,
			boolean createSchema) throws SQLException
	{
		persist = new Persist();
		persist.setCreateSchema(createSchema);
		persist.initialize(driver, connectionstring, username, password);
	}

	/**
	 * Constructor that omits driver class name. Only works with JDBC 4.0
	 * compliant drivers.
	 * 
	 * @param connectionstring
	 *            the connection string to the database.
	 * @param username
	 *            the database username.
	 * @param password
	 *            the database password.
	 * @throws SQLException
	 */
	public PersistenceManager(String connectionstring, String username, String password) throws SQLException
	{
		this(connectionstring, username, password, true);
	}

	/**
	 * Constructor that omits driver class name. Only works with JDBC 4.0
	 * compliant drivers.
	 * 
	 * @param connectionstring
	 *            the connection string to the database.
	 * @param username
	 *            the database username.
	 * @param password
	 *            the database password.
	 * @param createSchema
	 *            whether to create database tables or not.
	 * @throws SQLException
	 */
	public PersistenceManager(String connectionstring, String username, String password, boolean createSchema)
			throws SQLException
	{

		this(null, connectionstring, username, password, createSchema);
	}

	/**
	 * Delete one particular object from the database.
	 * 
	 * @param toDelete the object that will be deleted
	 * @return true if the object existed and was deleted, false otherwise.
	 * @throws SQLExcpetion
	 */
	public boolean deleteObject(Object toDelete) throws SQLException
	{
		boolean res = false;
		
		Long dbId = persist.getCache().getDatabaseId(toDelete);
		if(dbId != null)
		{
			res = persist.deleteObject(toDelete.getClass(), dbId);
		}
		
		return res;		
	}
	
	/**
	 * Delete one particular object from the database.
	 * 
	 * @param toDelete the object that will be deleted
	 * @param cw the connection wrapper to use for the operation.
	 * @return true if the object existed and was deleted, false otherwise.
	 * @throws SQLExcpetion
	 */
	public boolean deleteObject(Object toDelete, ConnectionWrapper cw) throws SQLException
	{
		boolean res = false;
		
		Long dbId = persist.getCache().getDatabaseId(toDelete);
		if(dbId != null)
		{
			res = persist.deleteObject(toDelete.getClass(), dbId,cw);
		}
		
		return res;		
	}

	/**
	 * Delete all objects that share properties with pattern. Convenience method
	 * that does not require the user to supply a ConnectionWrapper.
	 * 
	 * @param pattern
	 * @return the number of deleted objects.
	 */
	public int deleteObjects(Object pattern) throws SQLException
	{
		return persist.deleteObjects(pattern.getClass(), new Equal(pattern));
	}

	/**
	 * Delete all objects that share properties with pattern.
	 * 
	 * @param pattern
	 * @param cw
	 *            the connection wrapper to use for this operation.
	 * @return the number of deleted objects.
	 */
	public int deleteObjects(ConnectionWrapper cw, Object pattern) throws SQLException
	{
		return persist.deleteObjects(cw, pattern.getClass(), new Equal(pattern));
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
	public <T> int deleteObjects(Class<T> clazz, Clause where) throws SQLException
	{
		return persist.deleteObjects(clazz, where);
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
	public <T> int deleteObjects(ConnectionWrapper cw, Class<T> clazz, Clause where) throws SQLException
	{
		return persist.deleteObjects(cw, clazz, where);
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
	public void saveObject(Object object) throws SQLException
	{
		persist.saveObject(object);
	}

	/**
	 * Add an object to the database. If the object already exists, it will be
	 * updated.
	 * 
	 * @param object
	 *            the object to save.
	 * @param cw
	 *            the connection wrapper to use for this operation.
	 * 
	 * @throws SQLException
	 */
	public void saveObject(ConnectionWrapper cw, Object object) throws SQLException
	{
		persist.saveObject(cw, object, true, null);
	}

	/**
	 * Get all objects that share the non-null properties of pattern. If no
	 * results are found, an empty ArrayList is returned. Convenience method
	 * that does not require the user to supply a ConnectionWrapper.
	 * 
	 * @param pattern
	 *            the example to use for retrieving objects.
	 * @return a list of objects that match the pattern.
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	public <T> List<T> getObjectsMatching(T pattern) throws SQLException
	{
		return persist.getObjects((Class<T>) pattern.getClass(), new Equal(pattern, (Class<T>) pattern.getClass()));
	}

	/**
	 * Get the objects that match the non-null properties of pattern. The fields
	 * with non-null values in the pattern are matched to database fields.
	 * Convenience method that does not require the user to supply a
	 * ConnectionWrapper.
	 * 
	 * @param clazz
	 *            the class of the objects to look for.
	 * @param pattern
	 *            return only objects that match the pattern object.
	 */
	public <T> List<T> getObjectsMatching(Class<T> clazz, Object pattern) throws SQLException
	{
		return persist.getObjects(clazz, new Equal(pattern, clazz));
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
	public <T> List<T> getObjects(Class<T> clazz, Clause... clause) throws SQLException
	{
		return persist.getObjects(clazz, clause);
	}

	/**
	 * Get all objects that share the non-null properties of pattern. If no
	 * results are found, an empty ArrayList is returned.
	 * 
	 * @param pattern
	 *            the example to use for retrieving objects.
	 * @param cw
	 *            the wrapped connection to use for this operation.
	 * @return a list of objects that match the pattern.
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	public <T> List<T> getObjectsMatching(ConnectionWrapper cw, T pattern) throws SQLException
	{
		return persist.getObjects(cw, (Class<T>) pattern.getClass(), new Equal(pattern, (Class<T>) pattern.getClass()));
	}

	/**
	 * Get the objects that match the non-null properties of pattern. The fields
	 * with non-null values in the pattern are matched to database fields.
	 * 
	 * @param clazz
	 *            the class of the objects to look for.
	 * @param pattern
	 *            return only objects that match the pattern object.
	 */
	public <T> List<T> getObjectsMatching(ConnectionWrapper cw, Class<T> clazz, Object pattern) throws SQLException
	{
		return persist.getObjects(cw, clazz, new Equal(pattern, clazz));
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
	public <T> List<T> getObjects(ConnectionWrapper cw, Class<T> clazz, Clause... clause) throws SQLException
	{
		return persist.getObjects(cw, clazz, clause);
	}

	/**
	 * Get the number of objects that share the non-null properties of pattern.
	 * If no results are found, zero is returned. Convenience method that does
	 * not require the user to supply a ConnectionWrapper.
	 * 
	 * @param pattern
	 *            the example to use for retrieving objects.
	 * @return the number of objects that match the pattern.
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	public <T> long getCount(T pattern) throws SQLException
	{
		return getCount((Class<T>) pattern.getClass(), pattern);
	}

	/**
	 * Get the number of objects that share the non-null properties of pattern.
	 * If no results are found, zero is returned.
	 * 
	 * @param pattern
	 *            the example to use for retrieving objects.
	 * @param cw
	 *            the ConnectionWrapper to use for this transaction.
	 * @return the number of objects that match the pattern.
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	public <T> long getCount(ConnectionWrapper cw, T pattern) throws SQLException
	{
		return getCount(cw, (Class<T>) pattern.getClass(), pattern);
	}

	/**
	 * Get the number of objects that match the non-null properties of pattern.
	 * The fields with non-null values in the pattern are matched to database
	 * fields. Convenience method that does not require the user to supply a
	 * ConnectionWrapper.
	 * 
	 * @param clazz
	 *            the class of the objects to look for.
	 * @param pattern
	 *            return only objects that match the pattern object.
	 */
	public long getCount(Class<?> clazz, Object pattern) throws SQLException
	{
		return persist.getCount(clazz, new Equal(pattern, clazz));
	}

	/**
	 * Get the number of objects that match the non-null properties of pattern.
	 * The fields with non-null values in the pattern are matched to database
	 * fields.
	 * 
	 * @param clazz
	 *            the class of the objects to look for.
	 * @param pattern
	 *            return only objects that match the pattern object.
	 * @param cw
	 *            the connection wrapper to use for this transaction.
	 */
	public long getCount(ConnectionWrapper cw, Class<?> clazz, Object pattern) throws SQLException
	{
		return persist.getCount(cw, clazz, new Equal(pattern, clazz));
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
	public <T> long getCount(Class<T> clazz, Clause clause) throws SQLException
	{
		return persist.getCount(clazz, clause);
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
	public <T> long getCount(ConnectionWrapper cw, Class<T> clazz, Clause clause) throws SQLException
	{
		return persist.getCount(cw, clazz, clause);
	}

	/**
	 * Get the object of class clazz with the given database id. The actual
	 * object returned may be an instance of a subclass.
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
	public <T> T getObject(ConnectionWrapper cw, Class<T> clazz, Long id) throws SQLException, ClassNotFoundException
	{
		return persist.getObject(cw, clazz, id);
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
		return persist.getClasses();
	}

	/**
	 * Force a refresh of the object from the database, ignoring the cached
	 * values for the object. All objects that make up this object are also
	 * refreshed, which will affect all objects that share referenced objects
	 * with this object. The new object is returned. If the corresponding object
	 * has been deleted from the database, null is returned.
	 * 
	 * If the argument is not managed by Conserve, the method will throw an
	 * IllegalArumentException.
	 * 
	 * This is a convenience method for
	 * {@link #refresh(ConnectionWrapper, Object)}, a ConnectionWrapper will be
	 * automatically obtained and released.
	 * 
	 * @param obj
	 *            the object to refresh.
	 * @throws IllegalArumentException
	 *             if obj is not known by this instance of Conserve.
	 * @return the refreshed object or null if the object has been deleted from
	 *         the database.
	 * @throws SQLException
	 * 
	 */
	public <T> T refresh(T obj) throws IllegalArgumentException, SQLException
	{
		T res = null;
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			res = refresh(cw, obj);
			cw.commitAndDiscard();
		}
		catch (IllegalArgumentException iae)
		{
			// rethrow
			throw iae;
		}
		catch (Exception e)
		{
			// cancel the operation
			cw.rollbackAndDiscard();
			// re-throw the original exception
			throw new SQLException(e);
		}
		return res;
	}

	/**
	 * Force a refresh of the object from the database, ignoring the cached
	 * values for the object. All objects that make up this object are also
	 * refreshed, which will affect all objects that share referenced objects
	 * with this object. The new object is returned. If the corresponding object
	 * has been deleted from the database, null is returned.
	 * 
	 * If the argument is not managed by Conserve, the method will throw an
	 * IllegalArumentException.
	 * 
	 * @param cw
	 *            the connection wrapper to use.
	 * @param obj
	 *            the object to refresh.
	 * @throws IllegalArumentException
	 *             if obj is not known by this instance of Conserve.
	 * @return the refreshed object or null if the object has been deleted from
	 *         the database.
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public <T> T refresh(ConnectionWrapper cw, T obj) throws IllegalArgumentException, SQLException
	{
		return persist.refresh(cw, obj);
	}

	/**
	 * Check if an object has been changed since it was loaded from the
	 * database.
	 * 
	 * @param o
	 *            the object to check for, will be unchanged.
	 * 
	 * @return true if the object or any of its properties has changed or been
	 *         deleted, false otherwise.
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public boolean hasChanged(Object o) throws SQLException, ClassNotFoundException
	{
		Long dbId = persist.getCache().getDatabaseId(o);
		if (dbId != null)
		{
			ConnectionWrapper cw = null;
			try
			{
				cw = persist.getConnectionWrapper();
				// Search using o as example, make sure returned object exists
				// and has same table id number.
				HashMap<Class<?>, List<Long>> res = persist.getObjectDescriptors(o.getClass(), null, new Equal(o),
						null, cw);
				List<Long> ids = res.get(o.getClass());
				if (ids!=null && ids.contains(dbId))
				{
					return false;
				}
				else
				{
					return true;
				}
			}
			finally
			{
				if (cw != null)
				{
					cw.commitAndDiscard();
				}
			}
		}
		return false;
	}

	/**
	 * Drop all tables that make up class c. Tables will be dropped regardless
	 * if they are empty or not. All subclasses of c will also be dropped. If c
	 * is an interface, all classes that implement c will be dropped, along with
	 * their subclasses. Any interface that extends c will also be dropped,
	 * along with implementing classes and their sublcasses.
	 * 
	 * Warning: All classes that references c will also be dropped.
	 * 
	 * This is an extremely powerful method, use caution.
	 * 
	 * This method is a wrapper around
	 * {@link #dropTable(ConnectionWrapper, Class)}, a ConnectionWrapper will be
	 * automatically obtained and released.
	 * 
	 * @param c
	 *            the class of object to drop the table for.
	 * @throws SQLException
	 */
	public void dropTable(Class<?> c) throws SQLException
	{
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			dropTable(cw, c);
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
	 * Drop all tables that make up class c. Tables will be dropped regardless
	 * if they are empty or not. All subclasses of c will also be dropped. If c
	 * is an interface, all classes that implement c will be dropped, along with
	 * their subclasses. Any interface that extends c will also be dropped,
	 * along with implementing classes and their sublcasses.
	 * 
	 * Warning: All classes that references c will also be dropped.
	 * 
	 * This is an extremely powerful method, use caution.
	 * 
	 * 
	 * @param cw
	 *            the connection wrapper to use for the operation.
	 * @param c
	 *            the class of object to drop the table for.
	 * @throws SQLException
	 */
	public void dropTable(ConnectionWrapper cw, Class<?> c) throws SQLException
	{
		// delete all the objects of this class
		// this ensures that all dependent objects are cleared, e.g. arrays.
		deleteObjects(cw, c, new All());
		persist.getTableManager().dropTableForClass(c, cw);
	}

	/**
	 * Duplicate all entries from this PersistenceManager to the other. At the
	 * end of a successful call of this method the contents of the target
	 * database will be an exact duplicate the this database, with the exception
	 * of the id numbers.
	 * 
	 * The behaviour of this call is undefined if this PersistenceManager
	 * contains a code structure that is not compatible with the source
	 * PersistenceManager.
	 * 
	 * Useful for backing up a database, moving it from one server to another,
	 * or merging two databases.
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
	 *            the database to copy to.
	 * @throws SQLException
	 *             if anything goes wrong.
	 */
	public void duplicate(PersistenceManager target) throws SQLException
	{
		this.persist.duplicate(target.persist);

	}

	/**
	 * Update the database description or schema for the given class and all
	 * classes it depends on. This method is used after you have made changes to
	 * the definition of a class. If this method is called with a class that has
	 * not been changed it has no effect.
	 * 
	 * The following changes ARE supported:
	 * <p/>
	 * 
	 * * Add a property.
	 * <p/>
	 * 
	 * * Remove a property.
	 * <p/>
	 * 
	 * * Rename a property.
	 * <p/>
	 * 
	 * * Change a property from a primitive to the corresponding reference type,
	 * for example from double to Double. The opposite (Double to double) is
	 * possible, but not encouraged as it is entirely up to your code how any
	 * nulls existing in the database are handled.
	 * <p/>
	 * 
	 * * Change a property from one reference type to another. If the new type
	 * is a supertype or implemented by the original type, either directly or
	 * indirectly, references will be preserved. In other words, properties can
	 * be made more general, not less. For example, it is possible to change
	 * from ArrayList to List, but not the other way around, without losing
	 * data. Any non-compatible references will be dropped. If a property refers
	 * to both List and ArrayList objects and is converted from List to
	 * ArrayList (the 'wrong' way) all List references that are not also
	 * ArrayList references will be deleted.
	 * <p/>
	 * 
	 * * Change a reference type to a primitive, or the other way around, for
	 * example from String to java.util.Date. Please observe that this WILL
	 * result in all old references being null. This has the same effect as
	 * dropping the property and creating a new one. You probably don't want
	 * this, but you can. This does not apply to the reference types that
	 * directly correspond to primitive types (see above).
	 * <p/>
	 * 
	 * 
	 * 
	 * To carry out any of the supported changes, just pass a Class object you
	 * want changed to this method. If you are changing a property from
	 * primitive to reference or the other way, you do not even need to call
	 * this method, just start using the new class.
	 * <p/>
	 * 
	 * If you wish to implement any other changes, you have to do this in a
	 * two-step approach:
	 * <p/>
	 * 
	 * 1. Create an intermediary class, and read all of the old objects into it.
	 * Store the objects as intermediary classes.
	 * <p/>
	 * 
	 * 2. Drop the old class, and copy from the intermediary class to the new
	 * class, storing it.
	 * <p/>
	 * 
	 * In this case you do not use the updateSchema method.
	 * 
	 * 
	 * @param klass
	 * @throws SQLException
	 */
	public void updateSchema(Class<?> klass) throws SQLException
	{
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			persist.getTableManager().updateTableForClass(klass, cw);
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
	public void changeName(Class<?> oldClass, Class<?> newClass) throws SQLException
	{
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			persist.getTableManager().setTableName(oldClass, newClass, cw);
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
	 * Close the database connection and release all resources. After calling
	 * this method any further use of this object is undefined.
	 */
	public void close()
	{
		persist.close();
		persist = null;
	}

	/**
	 * Get a wrapped SQL connection to the database used by this
	 * PersistenceManager instance. To use the connection, call the
	 * getConnection() method on the returned object.
	 * 
	 * After you're done with the connection, it is important that you return it
	 * to the connection pool - otherwise your application will soon run out of
	 * connections.
	 * 
	 * There are two ways of doing this:
	 * 
	 * 1. Call rollback(), then discard() on the ConnectionWrapper. This undoes
	 * all your changes.
	 * 
	 * 2. Call commit(), then discard() on the ConnectionWrapper. This makes
	 * your changes permanent.
	 * 
	 * There are convenience methods that combine the two calls;
	 * rollbackAndDiscard() and commitAndDiscard().
	 * 
	 * After calling discard() you should no longer use the ConnectionWrapper or
	 * the associated connection, but instead request a new ConnectionWrapper
	 * using the getConnectionWrapper() method.
	 * 
	 * @return a ready-to-use ConnectionWrapper object.
	 * @throws SQLException
	 */
	public ConnectionWrapper getConnectionWrapper() throws SQLException
	{
		return persist.getConnectionWrapper();
	}

	/**
	 * Returns an array containing the result of the SQL sum() function for each field.
	 * If the field is an integer type, the corresponding entry is Integer or Long type, whichever is most appropriate.
	 * 
	 * If the field is a floating point type, the corresponding entry will be Float or Double, whichever is most appropriate.
	 * 
	 * 
	 * 
	 * @param cw the database connection to use for the operation.
	 * @param clazz the class of the object to calculate the sum for.
	 * @param fieldNames the names of the fields to calculate the sum for.
	 * @param where selection clauses that determine what objects will be matched - if empty, all objects are matched.
	 * 
	 * @return
	 * @throws SQLException 
	 */
	public Number[] getSum(ConnectionWrapper cw,Class<?>clazz, String [] fieldNames,Clause... where)throws SQLException
	{
		return persist.getSum(cw,clazz,fieldNames,where);
	}
	/**
	 * Convenience function that calculates the sum of one given field in all matching entries.
	 * 
	 * @param cw the database connection to use for the operation.
	 * @param clazz the class of the object to calculate the sum for.
	 * @param fieldName the name of the field to calculate the sum for.
	 * @param where selection clauses that determine what objects will be matched - if empty, all objects are matched.
	 * 
	 * @return
	 * @throws SQLException 
	 */
	public Number getSum(ConnectionWrapper cw,Class<?>clazz, String fieldName,Clause... where)throws SQLException
	{
		Number [] tmp = getSum(cw, clazz,new String []{fieldName},where);
		return tmp[0];
	}
	/**
	 * Returns an array containing the result of the SQL sum() function for each field.
	 * If the field is an integer type, the corresponding entry is Integer or Long type, whichever is most appropriate.
	 * 
	 * If the field is a floating point type, the corresponding entry will be Float or Double, whichever is most appropriate.
	 * 
	 * @param clazz the class of the object to calculate the sum for.
	 * @param fieldNames the names of the fields to calculate the sum for.
	 * @param where selection clauses that determine what objects will be matched - if empty, all objects are matched.
	 * 
	 * @return
	 * @throws SQLException 
	 */
	public Number[] getSum(Class<?>clazz, String [] fieldNames,Clause... where) throws SQLException
	{
		Number[] res=null;
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			res = getSum(cw,clazz,fieldNames,where);
			cw.commitAndDiscard();
		}
		catch (Exception e)
		{
			// cancel the operation
			cw.rollbackAndDiscard();
			// re-throw the original exception
			throw new SQLException(e);
		}	
		return res;
	}
	
	/**
	 * Convenience function that calculates the sum of one given field in all matching entries.
	 * 
	 * @param clazz the class of the object to calculate the sum for.
	 * @param fieldName the name of the field to calculate the sum for.
	 * @param where selection clauses that determine what objects will be matched - if empty, all objects are matched.
	 * 
	 * @return
	 * @throws SQLException 
	 */
	public Number getSum(Class<?>clazz, String fieldName,Clause... where) throws SQLException
	{
		Number [] tmp = getSum(clazz,new String []{fieldName},where);
		return tmp[0];
	}
}
