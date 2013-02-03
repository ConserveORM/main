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


/**
 * Runs the integration tests against a Derby/JavaDB database.
 * 
 * @author Erik Berglund
 *
 */
public class DerbyPersistTest extends PersistTest
{

	/**
	 * @see org.conserve.PersistTest#setUp()
	 */
	@Override
	public void setUp() throws Exception
	{
		driver = "org.apache.derby.jdbc.EmbeddedDriver";
		database = "jdbc:derby:derbyDB;create=true";
		secondDatabase = "jdbc:derby:derbyDB2;create=true";
		login = "";
		password = "";
		deleteAll();
	}
	
}