package org.conserve;


import org.conserve.tools.CaseInsensitiveStringMapTest;
import org.conserve.tools.CompabilityCalculatorTest;
import org.conserve.tools.ObjectRepresentationTest;
import org.conserve.tools.ObjectToolsTest;
import org.conserve.tools.UniqueIdGeneratorTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Test suite that runs all unit tests.
 * 
 * @author Erik Berglund
 *
 */

@RunWith(Suite.class)
@SuiteClasses({ 
	ObjectRepresentationTest.class, 
	ObjectToolsTest.class, 
	UniqueIdGeneratorTest.class,
	CompabilityCalculatorTest.class,
	CaseInsensitiveStringMapTest.class})
public class UnitTests
{

}
