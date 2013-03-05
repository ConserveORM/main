package org.conserve.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.conserve.tools.metadata.ChangeDescription;
import org.conserve.tools.metadata.MetadataException;
import org.conserve.tools.metadata.ObjectRepresentation;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * @author Erik Berglund
 * 
 */
public class ObjectRepresentationTest
{
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testGetDifference() throws Exception
	{
		// generate two identical reps
		ObjectRepresentation a = new ObjectRepresentation()
		{
		};
		ObjectRepresentation b = new ObjectRepresentation()
		{
		};
		a.addValueTrio("foo", null, Integer.class);
		a.addValueTrio("bar", null, String.class);
		a.addValueTrio("baz", null, Double.class);
		b.addValueTrio("foo", null, Integer.class);
		b.addValueTrio("bar", null, String.class);
		b.addValueTrio("baz", null, Double.class);

		assertNull(a.getDifference(b));
		assertNull(b.getDifference(a));
		assertNull(a.getDifference(a));
		assertNull(b.getDifference(b));

		// generate deletion rep
		b.removeProperty("bar");
		ChangeDescription cd = a.getDifference(b);
		assertNotNull(cd);
		assertEquals("bar", cd.getFromName());
		assertTrue(cd.isDeletion());

		// generate addition rep
		cd = b.getDifference(a);
		assertNotNull(cd);
		assertEquals("bar", cd.getToName());
		assertTrue(cd.isCreation());

		// generate rename rep
		b.addValueTrio("barz", null, String.class);
		cd = a.getDifference(b);
		assertNotNull(cd);
		assertEquals("bar", cd.getFromName());
		assertEquals("barz", cd.getToName());

		// generate change type rep
		b.removeProperty("barz");
		b.addValueTrio("bar", null, Long.class);
		cd = a.getDifference(b);
		assertNotNull(cd);
		assertEquals("bar", cd.getFromName());
		assertEquals("bar", cd.getToName());
		assertEquals(String.class, cd.getFromClass());
		assertEquals(Long.class, cd.getToClass());

		exception.expect(MetadataException.class);
		cd = a.getDifference(b);
		// generate two deletion reps
		// generate deletion rep and addition rep
		// generate deletion rep and change name rep
		// generate deletion rep and change type rep
		// generate two addition reps
		// generate addition rep and change name rep
		// generate addition rep and change type rep
		// generate two rename reps
		// generate rename rep and change type rep
		// generate two change type reps

		// generate rep that changes both type and name of one prop
	}

}
