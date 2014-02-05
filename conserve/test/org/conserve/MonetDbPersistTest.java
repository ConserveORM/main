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
 * Run test suite against a MonetDB database.
 * 
 * @author Erik Berglund
 *
 */
public class MonetDbPersistTest extends PersistTest
{

	/**
	 * @see org.conserve.DerbyPersistTest#setUp()
	 */
	@Override
	public void setUp() throws Exception
	{
		driver = "nl.cwi.monetdb.jdbc.MonetDriver";
		database = "jdbc:monetdb://localhost/test";
		secondDatabase = "jdbc:monetdb://localhost/test2";
		login = "monetdb";
		password = "monetdb";
		deleteAll();
	}

}
