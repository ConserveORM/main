package org.conserve;

/**
 * Runs the integration test script on the H2 database engine.
 * @author Erik Berglund
 *
 */
public class H2Test extends PersistTest
{

	@Override
	protected void setupConnectionConstants()
	{
		driver = "org.h2.Driver";
		database = "jdbc:h2:tcp://localhost/~/test";
		secondDatabase = "jdbc:h2:tcp://localhost/~/test2";
		login = "sa";
		password = "";
	}

}
