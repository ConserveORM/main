/*******************************************************************************
 *  
 * Copyright (c) 2009, 2018 Erik Berglund.
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

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Test suite that runs all integration tests.
 * 
 * @author Erik Berglund
 */

@RunWith(Suite.class)
@SuiteClasses({ 
	DerbyPersistTest.class, 
	FirebirdPersistTest.class, 
	HsqldbPersistTest.class, 
	//MariaDBPersistTest.class,
	MonetDbPersistTest.class, 
	MySQLPersistTest.class, 
	H2Test.class, 
	PostgreSQLPersistTest.class,
	SqLitePersistTest.class
	})
public class IntegrationTests
{

}
