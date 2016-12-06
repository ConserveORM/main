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
package com.github.conserveorm.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.conserveorm.tools.Defaults;

/**
 * A pool that maintains a set of open database connections.
 * 
 * @author Erik Berglund
 * 
 */
public class DataConnectionPool
{
	// database vars
	private String dataBase;// the database to connect to
	private String userName;// the user name to give when connecting to the
							// database
	private String password;// the password to give to the database

	private ArrayList<ConnectionWrapper> pool;
	private int lastConnection = 0;

	private Object mutex = new Object();

	private static final Logger LOGGER = Logger.getLogger(Defaults.LOGGER_NAME);


	/**
	 * Creates a new pool
	 * 
	 * @param poolsize
	 *            the initial size of the pool
	 * @param driver
	 *            the JDBC driver class name to use for the pool connections,
	 *            can be null if JDBC version is 4 or greater.
	 * @param db
	 *            The name of the database
	 * @param uname
	 *            The login name, can be null if the database allows it.
	 * @param pw
	 *            The login password, can be null if the database allows it.
	 */
	public DataConnectionPool(int poolsize, String driver, String db, String uname, String pw) throws SQLException
	{
		this.dataBase = db;
		this.userName = uname;
		this.password = pw;
		this.pool = new ArrayList<ConnectionWrapper>();

		if (driver != null)
		{
			try
			{
				// this is here for compatibility with drivers that are not JDBC
				// 4.0 compliant or better.
				Class.forName(driver).newInstance();
			}
			catch (InstantiationException | IllegalAccessException | ClassNotFoundException e1)
			{
				//re-throw
				throw new SQLException(e1);
			}
		}

		// set up the connections
		for (int x = 0; x < poolsize; x++)
		{
			if ((this.dataBase == null) )
			{
				throw new SQLException("Connection string must be given.");
			}
			else
			{
				try
				{
					pool.add(new ConnectionWrapper(DriverManager.getConnection(this.dataBase, this.userName, this.password)));
				}
				catch (Exception e)
				{
					//re-throw
					throw new SQLException(e);
				}
			}

		}
	}

	/**
	 * gets a new database connection allocates more connections if necessary
	 * use commit() or rollback() methods when you are done with it
	 * 
	 * @return an new ConnectionWrapper to the database
	 * @throws SQLException
	 */
	public ConnectionWrapper getConnectionWrapper() throws SQLException
	{
		synchronized (this.mutex)
		{
			int x = this.lastConnection;
			ConnectionWrapper res = null;
			for (int y = 0; y < pool.size(); y++)
			{
				x++;
				x %= pool.size();
				if (!pool.get(x).isTaken())
				{
					res = pool.get(x);
					res.setTaken(true);
					this.lastConnection = x;
					break;
				}
			}
			if (res == null)// try increasing the pool size if no connection was available
			{
				this.increasePoolSizePercent(10);// increase poolsize with
													// 10%
				x = this.lastConnection;
				for (int y = 0; y < pool.size(); y++)
				{
					x++;
					x %= pool.size();
					if (!pool.get(x).isTaken())
					{
						res = pool.get(x);
						res.setTaken(true);
						this.lastConnection = x;
						break;
					}
				}
			}
			if (res == null)
			{
				LOGGER.warning("Pool size increase failed, or something else went wrong.");
			}
			return res;
		}
	}

	private void increasePoolSizePercent(int increment) throws SQLException
	{
		synchronized (this.mutex)
		{
			double percincr = 1 + (increment / 100.0);
			int newsize = (int) (pool.size() * percincr);
			if (newsize <= pool.size())
			{
				newsize = pool.size() + 1;
			}
			LOGGER.fine("Increasing pool size from " + pool.size() + " to " + newsize + " connections.");
			while (pool.size() < newsize)
			{
				pool.add(new ConnectionWrapper(DriverManager.getConnection(this.dataBase, this.userName, this.password)));
			}
		}
	}


	// close all connections and get out of here
	public void cleanUp()
	{
		try
		{
			synchronized (this.mutex)
			{
				while (!pool.isEmpty())
				{
					pool.get(0).setTaken(true);
					Connection c = pool.get(0).getConnection();
					c.commit();
				
					//this is a workaround for a bug in some databases that 
					//won't properly close connections if they're not in autocommit mode
					c.setAutoCommit(true);
					
					c.close();
					pool.remove(0);
				}
				this.pool = null;
			}
		}
		catch (Exception e)
		{
			//We catch all exceptions, there's nothing to be done if we can't close a connection.
			LOGGER.log(Level.WARNING, "Exception: ", e);
		}
	}
}
