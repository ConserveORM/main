package org.conserve;

/**
 * @author Erik Berglund
 *
 */
public class MariaDBPersistTest extends PersistTest
{
	/**
	 * @see org.conserve.PersistTest#setUp()
	 */
	@Override
	public void setUp() throws Exception
	{
		driver = "org.mariadb.jdbc.Driver";
		database = "jdbc:mariadb://localhost/test";
		secondDatabase = "jdbc:mariadb://localhost/test2";
		login = "root";
		password = "root";
		deleteAll();
	}
}
