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
package org.conserve;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.conserve.connection.ConnectionWrapper;
import org.junit.Test;

/**
 * @author Erik Berglund
 * 
 */
public class SqLitePersistTest extends PersistTest
{

	/**
	 * @see org.conserve.PersistTest#setUp()
	 */
	@Override
	public void setUp() throws Exception
	{
		driver = "org.sqlite.JDBC";
		database = "jdbc:sqlite:sqlitetest.db";
		secondDatabase = "jdbc:sqlite:sqlitetest2.db";
		login = "sa";
		password = "";
		deleteAll();
	}

	/**
	 * Try reading from a database table.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testBasicFunctionality() throws Exception
	{

		PersistenceManager persist = new PersistenceManager(driver, database,
				login, password);
		ConnectionWrapper cw = persist.getConnectionWrapper();
		
		PreparedStatement stmt = cw.prepareStatement("SELECT COUNT(*) FROM C__ARRAY");
		ResultSet rs = stmt.executeQuery();
		if(rs.next())
		{
			System.out.println("Found " + rs.getInt(1) + " objects.");
		}
		else
		{
			System.out.println("No result.");
		}
		stmt.close();
		cw.commitAndDiscard();
		persist.close();
	}

}
