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


/**
 * Runs the integration test scripts against a HSQLDB database.
 * 
 * @author Erik Berglund
 *
 */
public class HsqldbPersistTest extends PersistTest
{

	@Override
	protected void setupConnectionConstants()
	{
		driver = "org.hsqldb.jdbcDriver";
		
		//embedded:
		database = "jdbc:hsqldb:file:hsqldbtest;shutdown=true";
		secondDatabase = "jdbc:hsqldb:file:hsqldbtest2;shutdown=true";
		
		//server:
		//database = "jdbc:hsqldb:hsql://localhost/hsqldbtest";
		//secondDatabase = "jdbc:hsqldb:hsql://localhost/hsqldbtest2";
		
		
		login = "sa";
		password = "";
		
		//HSQLDB resets ALL loggers
		//workaround:
		System.setProperty("hsqldb.reconfig_logging", "false");
	}
}
