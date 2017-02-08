/*******************************************************************************
 *  
 * Copyright (c) 2009, 2017 Erik Berglund.
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
package com.github.conserveorm;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.github.conserveorm.aggregate.AggregateFunction;
import com.github.conserveorm.cache.ObjectRowMap;
import com.github.conserveorm.connection.ConnectionWrapper;
import com.github.conserveorm.select.Clause;
import com.github.conserveorm.select.discriminators.Equal;
import com.github.conserveorm.tools.Defaults;
import com.github.conserveorm.tools.Tools;
import com.github.conserveorm.tools.generators.NameGenerator;
import com.github.conserveorm.tools.protection.ProtectionManager;

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
	 * property=value. The properties you can set are:
	 * com.github.conserveorm.driver, com.github.conserveorm.connectionstring, 
	 * com.github.conserveorm.username, and com.github.conserveorm.password.
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
	 * property=value. The properties you can set are:
	 * com.github.conserveorm.driver, com.github.conserveorm.connectionstring, 
	 * com.github.conserveorm.username, and com.github.conserveorm.password.
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
	 * Reads property=value pairs from the inputstream. The properties you can set are:
	 * com.github.conserveorm.driver, com.github.conserveorm.connectionstring, 
	 * com.github.conserveorm.username, and com.github.conserveorm.password.
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
	 * the database tables will be automatically created. The properties you can set are:
	 * com.github.conserveorm.driver, com.github.conserveorm.connectionstring, 
	 * com.github.conserveorm.username, and com.github.conserveorm.password.
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
	 * Create object, read settings from provided properties. The properties you can set are:
	 * com.github.conserveorm.driver, com.github.conserveorm.connectionstring, 
	 * com.github.conserveorm.username, and com.github.conserveorm.password.
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
	 * The properties you can set are:
	 * com.github.conserveorm.driver, com.github.conserveorm.connectionstring, 
	 * com.github.conserveorm.username, and com.github.conserveorm.password.
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
		
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			res = deleteObject(cw,toDelete);
			cw.commitAndDiscard();
		}
		catch(Exception e)
		{
			cw.rollbackAndDiscard();
			throw new SQLException(e);
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
	public boolean deleteObject(ConnectionWrapper cw, Object toDelete) throws SQLException
	{
		boolean res = false;
		
		Long dbId = persist.getCache().getDatabaseId(toDelete);
		if(dbId != null)
		{
			ProtectionManager pm = persist.getProtectionManager();
			pm.unprotectObjectExternal( dbId, cw);
			if(!pm.isProtected( dbId, cw))
			{
				res = persist.deleteObject(cw,toDelete.getClass(), dbId);
			}
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
		int res = 0;
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			res = deleteObjects(cw,pattern);
			cw.commitAndDiscard();
		}
		catch(Exception e)
		{
			cw.rollbackAndDiscard();
			throw new SQLException(e);
		}
		return res;
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
		int res = 0;
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			res = deleteObjects(cw,clazz,where);
			cw.commitAndDiscard();
		}
		catch(Exception e)
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
	public <T> int deleteObjects(ConnectionWrapper cw, Class<T> clazz, Clause where) throws SQLException
	{
		return persist.deleteObjects(cw, clazz, where);
	}

	/**
	 * Add an object to the database. If the object already exists, it will be
	 * updated. Convenience method that does not require the user to supply a
	 * ConnectionWrapper. The method returns the database id of the object. The
	 * database id is invariant across {@link PersistenceManager} instances.
	 * It's safe to ignore the return value.
	 * 
	 * This is a convenience method that handles getting and discarding the connection
	 * wrapper for you.
	 * 
	 * @param object
	 *            the object to save.
	 * 
	 * @return the database id of the saved object, or null if it could not be saved.
	 * 
	 * @throws SQLException
	 */
	public Long saveObject(Object object) throws SQLException
	{
		Long res = null;
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			res = saveObject(cw, object);
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
	 * Add an object to the database. If the object already exists, it will be
	 * updated. The method returns the database id of the object. The database
	 * id is invariant across {@link PersistenceManager} instances. It's safe to
	 * ignore the return value.
	 * 
	 * @param object
	 *            the object to save.
	 * @param cw
	 *            the connection wrapper to use for this operation.
	 *            
	 * @return the database id of the saved object, or null if it could not be saved.
	 * 
	 * @throws SQLException
	 */
	public Long saveObject(ConnectionWrapper cw, Object object)
			throws SQLException
	{
		return persist.saveObject(cw, object, true, null);
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
	public <T> List<T> getObjects(T pattern) throws SQLException
	{
		List<T>res = null;
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			res = getObjects(cw,pattern);
			cw.commitAndDiscard();
		}
		catch(Exception e)
		{
			cw.rollbackAndDiscard();
			throw new SQLException(e);
		}
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
	 *            the clause(s) that all the returned objects must satisfy.
	 * @return an ArrayList of the desired type.
	 * @throws SQLException
	 */
	public <T> List<T> getObjects(Class<T> clazz, Clause... clause) throws SQLException
	{
		List<T> res = null;
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			res = getObjects(cw,clazz,clause);
			cw.commitAndDiscard();
		}
		catch(Exception e)
		{
			cw.rollbackAndDiscard();
			throw new SQLException(e);
		}
		return res;
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
	public <T> List<T> getObjects(ConnectionWrapper cw, T pattern) throws SQLException
	{
		return getObjects(cw, (Class<T>) pattern.getClass(), pattern);
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
	public <T> List<T> getObjects(ConnectionWrapper cw, Class<T> clazz, Object pattern) throws SQLException
	{
		return persist.getObjects(cw, clazz, new Equal(pattern, clazz));
	}

	/**
	 * Get the objects matching the search class and search clauses. The objects
	 * are passed one by one to the {@link SearchListener#objectFound(Object)}
	 * method of the listener parameter.
	 * 
	 * This method conserves memory compared to the other getObjects(...)
	 * methods by only loading one object at a time. This means this method is
	 * slower than the other getObjects(...) methods, as a new database query is
	 * issued for each separate object.
	 * 
	 * The next object in the search won't be loaded until the objectFound(...)
	 * method returns, so if heavy processing needs to be done on each object
	 * it's best to offload it to a separate thread.
	 * 
	 * 
	 * @param listener
	 *            an object that implements the SearchListener interface.
	 * @param clazz
	 *            the class of objects to search for.
	 * @param clauses
	 *            the clause(s) that all returned objects must satisfy.
	 * @throws SQLException
	 */
	public <T> void getObjects(Class<T> clazz, SearchListener<T> listener, Clause... clauses) throws SQLException
	{
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			persist.getObjects(cw,listener, clazz, clauses);
			cw.commitAndDiscard();
		}
		catch(Exception e)
		{
			cw.rollbackAndDiscard();
			throw new SQLException(e);
		}
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
	public <T> long getCount(T pattern) throws SQLException
	{
		long res = 0;
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			res = getCount(cw,pattern);
			cw.commitAndDiscard();
		}
		catch(Exception e)
		{
			cw.rollbackAndDiscard();
			throw new SQLException(e);
		}
		return res;
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
	public <T> long getCount(Class<T> clazz, Clause... clause) throws SQLException
	{
		long res = 0;
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			res = getCount(cw,clazz,clause);
			cw.commitAndDiscard();
		}
		catch(Exception e)
		{
			cw.rollbackAndDiscard();
			throw new SQLException(e);
		}
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
	public <T> long getCount(ConnectionWrapper cw, Class<T> clazz, Clause... clause) throws SQLException
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
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public <T> T getObject(ConnectionWrapper cw, Class<T> clazz, Long id) throws SQLException, ClassNotFoundException
	{
		return persist.getObject(cw, clazz, id);
	}
	/**
	 * Get the object of class clazz with the given database id. The actual
	 * object returned may be an instance of a subclass.
	 * This is a convenience method that handles the ConnectionWrapper for you.
	 * 
	 * @param clazz
	 *            the class of the object to retrieve.
	 * @param id
	 *            the database id of the object at the level of clazz.
	 *            
	 * @return the matching object.
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public <T> T getObject( Class<T> clazz, Long id) throws SQLException, ClassNotFoundException
	{
		T res = null;
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			res = getObject(cw, clazz, id);
			cw.commitAndDiscard();
		}
		catch(Exception e)
		{
			cw.rollbackAndDiscard();
			throw new SQLException(e);
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
	public List<Class<?>> getClasses(ConnectionWrapper cw) throws SQLException
	{
		return persist.getClasses(cw);
	}
	
	/**
	 * Get a list of all classes persisted in this database. It does not include
	 * classes representing primitives, e.g. java.lang.Integer, or array
	 * classes.
	 * This is a convenience method that allocates a ConnectionWrapper for you.
	 * 
	 * @return a list of classes.
	 * @throws SQLException
	 */
	public List<Class<?>> getClasses() throws SQLException
	{
		List<Class<?>>res=null;
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			res = getClasses(cw);
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
	 * This is a convenience method that handles the ConnectionWrapper for you.
	 * 
	 * @param o
	 *            the object to check for, will be unchanged.
	 * 
	 * @return true if the object or any of its properties has changed or been
	 *         deleted, false otherwise.
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public boolean hasChanged( Object o) throws SQLException, ClassNotFoundException
	{
		boolean res = false;
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			res = hasChanged(cw,o);
			cw.commitAndDiscard();
		}
		catch(Exception e)
		{
			cw.rollbackAndDiscard();
			throw new SQLException(e);
		}
		return res;
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
	private boolean hasChanged(ConnectionWrapper cw, Object o) throws SQLException, ClassNotFoundException
	{
		Long dbId = persist.getCache().getDatabaseId(o);
		if (dbId != null)
		{
			Class<?>clazz = o.getClass();
			//get all classes that are subclasses of clazz, or equal to clazz
			List<Class<?>> allClasses = getClasses(cw);
			Iterator<Class<?>> iter = allClasses.iterator();
			while(iter.hasNext())
			{
				Class<?> tmpClass = iter.next();
				if(!clazz.isAssignableFrom(tmpClass))
				{
					iter.remove();
				}
			}
			// Search using o as example, make sure returned object exists
			// and has same table id number.
			HashMap<Class<?>, List<Long>> res = persist.getObjectDescriptors(cw, clazz, null,allClasses, new Equal(o), null);
			List<Long> ids = res.get(o.getClass());
			if (ids != null && ids.contains(dbId))
			{
				boolean result = true;
				// get the object from the database
				ObjectRowMap tmpCache = new ObjectRowMap();
				tmpCache.start();
				Object actual = persist.getObject(cw, Object.class, dbId, tmpCache);
				tmpCache.stop();
				tmpCache = new ObjectRowMap();
				tmpCache.start();
				// temporarily save the old object, bypassing the cache
				long tmpId = persist.saveObject(cw, o, false, null, tmpCache);
				// make sure the new object can be used to find the old object
				res = persist.getObjectDescriptors(cw, clazz, null, allClasses,new Equal(actual), null);
				ids = res.get(o.getClass());
				if (ids != null && ids.contains(tmpId))
				{
					result = false;
				}
				// delete the temporary object by rolling back the transaction
				cw.rollback();
				tmpCache.stop();
				// purge temporary object from cache
				persist.getCache().purge(NameGenerator.getTableName(o, persist.getAdapter()), tmpId);
				return result;
			}
			else
			{
				return true;
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
		persist.getTableManager().dropTableForClass(c, cw);
	}
	
	/**
	 * Get the amount of the total capacity that has been used. Since Conserve
	 * stores objects with a unique, database generated identifier there is an
	 * upper limit to the number of objects that can be stored. This number is
	 * very, very large but nevertheless finite. As an example, most databases
	 * allows 64-bit signed integers as auto-generated identifiers. This gives
	 * 2^63-1 or 9,223,372,036,854,775,807 entries. That's nine quintillion, two
	 * hundred twenty-three quadrillion, three hundred seventy-two trillion,
	 * thirty-six billion, eight hundred fifty-four million, seven hundred
	 * seventy-five thousand, eight hundred seven. If you add a thousand entries
	 * every second it will take you more than 290 million years to run out.
	 * Some database engines (notably SQLite and its derivatives) use a smaller
	 * number here, so you can actually realistically run out.
	 * 
	 * Returns a number in the range [0,1]. 0 indicates that the database is
	 * empty, 1 means it's full.
	 * 
	 * Note that even if you delete objects this number
	 * won't necessarily decrease, as used identifiers may not be recycled.
	 * This behaviour is database dependent.
	 * 
	 * Note that if you delete everything in your database, this method will incorrectly
	 * return 0, even though some identifiers have been used up.
	 * 
	 * @return the fraction of total capacity that has been used, normalised to
	 *         the range [0,1].
	 * @throws SQLException
	 */
	public double getUsedCapacity(ConnectionWrapper cw) throws SQLException
	{
		String objectTableName = NameGenerator.getTableName(Object.class, persist.getAdapter());
		if (persist.getTableManager().tableExists(objectTableName, cw))
		{
			String query = "SELECT MAX(" + Defaults.ID_COL + ") FROM " + objectTableName;
			PreparedStatement prepareStatement = cw.prepareStatement(query);
			Tools.logFine(prepareStatement);
			ResultSet rs = prepareStatement.executeQuery();
			rs.next();
			long count = rs.getLong(1);
			return count / (double) persist.getAdapter().getMaximumIdNumber();
		}
		else
		{
			//if the table don't exist no capacity has been used
			return 0;
		}
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
	 * 
	 * * Remove or remove a property.
	 * <p/>
	 * 
	 * * Rename a property.
	 * <p/>
	 * 
	 * * Add or remove an index.
	 * <p/>
	 * 
	 * * Move a class from one superclass to another.
	 * <p/>
	 * 
	 * * Add or remove an interface.
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
	 * After calling this method the PersistenceManager should be closed and a new instance created.
	 * Any other PersistenceManager objects should do the same - the integrity of objects that are loaded 
	 * by other PersistenceManagers can not be guaranteed otherwise.
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
	 * Close the database connection and release all resources. After calling
	 * this method any further use of this object is undefined.
	 */
	public void close()
	{
		if (persist != null)
		{
			persist.close();
			persist = null;
		}
	}
	
	/**
	 * Package-level accessor for the managed Persistence object.
	 */
	public Persist getPersist()
	{
		return this.persist;
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
	 * Returns an array containing the result of the SQL aggregate function for each field.
	 * If the field is an integer type, the corresponding entry is  Long, Integer, Byte, or Short type, whichever is appropriate.
	 * 
	 * If the field is a floating point type, the corresponding entry will be Double or Float, whichever is appropriate.
	 * 
	 * If the function is Average, the corresponding entry in the returned array will be Double, no matter what the field type is.
	 * 
	 * If the function is Sum the corresponding entry in the returned array will be Double or Long, as appropriate.
	 * 
	 * This function is undefined for non-numeric fields.
	 * 
	 * @param cw the database connection to use for the operation.
	 * @param clazz the class of the object to calculate the sum for.
	 * functions the functions to calculate - each entry will get a corresponding entry in the returned array.
	 * @param where selection clauses that determine what objects will be matched - if empty, all objects are matched.
	 * 
	 * @return an array of Number subclasses, each corresponding to an entry in the functions parameter.
	 * @throws SQLException 
	 */
	public Number[] calculateAggregate(ConnectionWrapper cw,Class<?>clazz, AggregateFunction [] functions,Clause... where)throws SQLException
	{
		return persist.calculateAggregate(cw,clazz,functions,where);
	}
	
	/**
	 * Convenience function that calculates the sum of one given field in all matching entries.
	 * See {@link #calculateAggregate(ConnectionWrapper, Class, AggregateFunction[], Clause...)} for more detail.
	 * 
	 * @param cw the database connection to use for the operation.
	 * @param clazz the class of the object to calculate the sum for.
	 * @param function the function to calculate.
	 * @param where selection clauses that determine what objects will be matched - if empty, all objects are matched.
	 * 
	 * @return the result of the aggregate function, as a subclass of Number.
	 * @throws SQLException 
	 */
	public Number calculateAggregate(ConnectionWrapper cw,Class<?>clazz, AggregateFunction function,Clause... where)throws SQLException
	{
		Number [] tmp = calculateAggregate(cw, clazz,new AggregateFunction []{function},where);
		return tmp[0];
	}
	
	/**
	 * This function is a convenience function for {@link #calculateAggregate(ConnectionWrapper, Class, AggregateFunction[], Clause...)} 
	 * that handles requesting and discarding the ConnectionWrapper for you. See that function's comments for more detail.
	 * 
	 * @param clazz the class of the object to calculate the sum for.
	 * @param functions the functions to calculate - each entry will get a corresponding entry in the returned array.
	 * @param where selection clauses that determine what objects will be matched - if empty, all objects are matched.
	 * 
	 * @return an array of Number subclasses, each corresponding to an entry in the functions parameter.
	 * @throws SQLException 
	 */
	public Number[] calculateAggregate(Class<?>clazz, AggregateFunction [] functions,Clause... where) throws SQLException
	{
		Number[] res=null;
		ConnectionWrapper cw = getConnectionWrapper();
		try
		{
			res = calculateAggregate(cw,clazz,functions,where);
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
	 * See {@link #calculateAggregate(ConnectionWrapper, Class, AggregateFunction[], Clause...)} for more detail.
	 * 
	 * @param clazz the class of the object to calculate the sum for.
	 * @param function the function to calculate.
	 * @param where selection clauses that determine what objects will be matched - if empty, all objects are matched.
	 * 
	 * @return the result of the aggregate function, as a subclass of  Number.
	 * @throws SQLException 
	 */
	public Number calculateAggregate(Class<?>clazz, AggregateFunction function,Clause... where) throws SQLException
	{
		Number [] tmp = calculateAggregate(clazz,new AggregateFunction []{function},where);
		return tmp[0];
	}


	
}
