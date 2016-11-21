package org.conserve;

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
	SqLitePersistTest.class})
public class IntegrationTests
{

}
