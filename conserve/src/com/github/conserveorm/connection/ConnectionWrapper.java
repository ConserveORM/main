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
package com.github.conserveorm.connection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Class that wraps the connection so that there is a reasonable assurance that only one thread uses a connection at a
 * time.
 * 
 * @author Erik Berglund
 * 
 */
public class ConnectionWrapper
{

	private Connection c;// the connection this object is a wrapper for
	private boolean taken;// true if the connection is in use

	/**
	 * Class constructor.
	 * 
	 * @param con
	 *            the connection to store in this wrapper.
	 * @throws SQLException
	 */
	public ConnectionWrapper(Connection con) throws SQLException
	{
		this.c = con;
		this.taken = false;
		this.c.setAutoCommit(false);
	}

	/**
	 * Set whether this object is in use or not.
	 * 
	 * @param t
	 *            the new status of the object, true if it is in use, false otherwise.
	 * @throws SQLException
	 */
	protected synchronized void setTaken(boolean t) throws SQLException
	{
		if (t)
		{
			c.setAutoCommit(false);
		}
		this.taken = t;
	}

	/**
	 * Get the status of this object.
	 * 
	 * @return a boolean value indicating if the object is in use or not.
	 */
	public synchronized boolean isTaken()
	{

		return this.taken;
	}

	/**
	 * All transactions executed on this ConnectionWrapper's connection are
	 * committed, if they have not already been so.
	 * 
	 * @throws SQLException
	 */
	public void commit() throws SQLException
	{
		c.commit();
	}

	/**
	 * Rolls back all pending transactions on this ConnectionWrapper's connection.
	 * 
	 * @throws SQLException
	 */
	public void rollback() throws SQLException
	{
		c.rollback();
	}

	/**
	 * Return the connection to the pool so that it can be used again.
	 * Calling this method does not commit or rollback pending transactions.
	 * 
	 */
	public void discard()
	{
		try
		{
			setTaken(false);
		}
		catch (SQLException e)
		{
			// this never happens
		}
	}

	/**
	 * Roll back all pending transactions, then return the connection to the pool.
	 * 
	 * @throws SQLException
	 * 
	 */
	public synchronized void rollbackAndDiscard() throws SQLException
	{
		discard();
		rollback();
	}

	/**
	 * Commit all pending transactions, then return the connection to the pool.
	 * @throws SQLException 
	 * 
	 */
	public synchronized void commitAndDiscard() throws SQLException
	{
		discard();
		commit();
	}

	/**
	 * Accessor for the SQL connection.
	 * 
	 * This connection can be used directly by the application as any other JDBC connection.
	 * 
	 * Please observe the following caveats:
	 * 
	 * Don't close the connection. When you are done with it, call commitAndDiscard() rollbackAndDiscard() or discard() on the associated
	 * ConnectionWrapper.
	 * 
	 * Don't use the Connection object associated with a ConnectionWrapper after commitAndDiscard() rollbackAndDiscard() or discard() has been called
	 * on it. Instead, request a new ConnectionWrapper.
	 * 
	 * 
	 * @return the Connection wrapped by this object.
	 */
	public Connection getConnection()
	{
		return c;
	}

	/**
	 * @param statement
	 * @return a new PreparedStatement, ready to fill in values and execute.
	 * @throws SQLException 
	 */
	public PreparedStatement prepareStatement(String statement) throws SQLException
	{
		return getConnection().prepareStatement(statement);
	}

}
