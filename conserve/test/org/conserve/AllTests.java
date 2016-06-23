package org.conserve;

import org.conserve.tools.ObjectRepresentationTest;
import org.conserve.tools.ObjectToolsTest;
import org.conserve.tools.UniqueIdGeneratorTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	DerbyPersistTest.class, 
	FirebirdPersistTest.class, 
	HsqldbPersistTest.class, 
	//MariaDBPersistTest.class,
	MonetDbPersistTest.class, 
	MySQLPersistTest.class, 
	PersistTest.class, 
	PostgreSQLPersistTest.class,
	SqLitePersistTest.class, 
	ObjectRepresentationTest.class, 
	ObjectToolsTest.class, 
	UniqueIdGeneratorTest.class })
public class AllTests
{

}
