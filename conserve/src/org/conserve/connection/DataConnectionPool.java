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
package org.conserve.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

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

	private static final Logger LOGGER = Logger.getLogger("org.conserve");


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
	 *            The login name
	 * @param pw
	 *            The login password
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
				e1.printStackTrace();
			}
		}

		// set up the connections
		for (int x = 0; x < poolsize; x++)
		{
			if ((this.userName == null) || (this.dataBase == null) || (this.password == null))
			{
				throw new SQLException("Connection string, user name and password must be given. User name and password may be empty strings.");
			}
			else
			{
				try
				{
					pool.add(new ConnectionWrapper(DriverManager.getConnection(this.dataBase, this.userName, this.password)));
				}
				catch (Exception e)
				{
					LOGGER.log(Level.WARNING, "Exception: ", e);
					break;
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
			loop1: for (int y = 0; y < pool.size(); y++)
			{
				x++;
				x %= pool.size();
				if (!pool.get(x).isTaken())
				{
					res = pool.get(x);
					res.setTaken(true);
					this.lastConnection = x;
					break loop1;
				}
			}
			if (res == null)// try increasing the pool size if no connection was
							// available
			{
				try
				{
					this.increasePoolSizePercent(10);// increase poolsize with
														// 10%
					x = this.lastConnection;
					loop2: for (int y = 0; y < pool.size(); y++)
					{
						x++;
						x %= pool.size();
						if (!pool.get(x).isTaken())
						{
							res = pool.get(x);
							res.setTaken(true);
							this.lastConnection = x;
							break loop2;
						}
					}

				}
				catch (Exception e)
				{
					LOGGER.log(Level.WARNING, "Exception: ", e);
					// okay, there's nothing more to do
					return null;
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
			this.setPoolSize(newsize);
		}
	}

	private void setPoolSize(int newsize) throws SQLException
	{
		synchronized (this.mutex) // don't change pool size while it's in use
		{
			if (this.pool.size() < newsize) // increase the pool size
			{
				LOGGER.fine("Increasing pool size from " + pool.size() + " to " + newsize + " entries.");
				while (pool.size() < newsize)
				{
					pool.add(new ConnectionWrapper(DriverManager.getConnection(this.dataBase, this.userName, this.password)));
				}
			}
			else if (this.pool.size() > newsize)// decrease the pool size
			{
				LOGGER.fine("Decreasing pool size from " + pool.size() + " to " + newsize + " entries.");
				while (pool.size() > newsize)
				{
					// find the first unused connection
					boolean found = false;
					int x = 0;
					for (; x < pool.size(); x++)
					{
						if (!pool.get(x).isTaken())
						{
							pool.get(x).setTaken(true);
							pool.get(x).getConnection().setAutoCommit(true);
							pool.get(x).getConnection().close();
							pool.remove(x);
							found = true;
							break;
						}
					}
					if (!found)
					{
						// we have removed as many connections as we can, we
						// won't remove taken connections
						break;
					}
				}
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
			LOGGER.log(Level.WARNING, "Exception: ", e);
		}
	}
}
