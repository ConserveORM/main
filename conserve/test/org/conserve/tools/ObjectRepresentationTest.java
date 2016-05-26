package org.conserve.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.conserve.tools.metadata.FieldChangeDescription;
import org.conserve.tools.metadata.MetadataException;
import org.conserve.tools.metadata.ObjectRepresentation;
import org.junit.Test;

/**
 * @author Erik Berglund
 * 
 */
public class ObjectRepresentationTest
{

	@Test
	public void testGetDifference() throws Exception
	{
		// generate two identical reps
		ObjectRepresentation a = getStandard();
		ObjectRepresentation b = getStandard();

		assertNull(a.getFieldDifference(b));
		assertNull(b.getFieldDifference(a));
		assertNull(a.getFieldDifference(a));
		assertNull(b.getFieldDifference(b));

		// generate deletion rep
		b.removeProperty("bar");
		FieldChangeDescription cd = a.getFieldDifference(b);
		assertNotNull(cd);
		assertEquals("bar", cd.getFromName());
		assertTrue(cd.isDeletion());
		assertNull(b.getFieldDifference(b));

		// generate addition rep
		cd = b.getFieldDifference(a);
		assertNotNull(cd);
		assertEquals("bar", cd.getToName());
		assertTrue(cd.isCreation());

		// generate rename rep
		b.addNamedType("barz",  String.class);
		cd = a.getFieldDifference(b);
		assertNotNull(cd);
		assertEquals("bar", cd.getFromName());
		assertEquals("barz", cd.getToName());
		assertNull(b.getFieldDifference(b));

		// generate change type rep
		b.removeProperty("barz");
		b.addNamedType("bar",  Long.class);
		cd = a.getFieldDifference(b);
		assertNotNull(cd);
		assertEquals("bar", cd.getFromName());
		assertEquals("bar", cd.getToName());
		assertEquals(String.class, cd.getFromClass());
		assertEquals(Long.class, cd.getToClass());
		assertNull(b.getFieldDifference(b));

		// generate two deletion reps
		b.removeProperty("bar");
		b.removeProperty("baz");
		expectMetdataException(a, b);
		// generate two addition reps
		expectMetdataException(b, a);
		assertNull(b.getFieldDifference(b));
		
		// generate deletion rep and addition rep
		b.addNamedType("baz",  Double.class);
		b.addNamedType("bat",  Double.class);
		expectMetdataException(a, b);
		expectMetdataException(b, a);
		assertNull(b.getFieldDifference(b));
		
		// generate deletion rep and change name rep
		b = getStandard();
		b.removeProperty("bar");
		b.removeProperty("baz");
		b.addNamedType("fut",  Double.class);
		expectMetdataException(a, b);
		// generate addition rep and change name rep
		expectMetdataException(b, a);
		assertNull(b.getFieldDifference(b));
		
		// generate deletion rep and change type rep
		b = getStandard();
		b.removeProperty("bar");
		b.addNamedType("bar", Float.class);
		b.removeProperty("baz");
		expectMetdataException(a, b);
		// generate addition rep and change type rep
		expectMetdataException(b, a);
		assertNull(b.getFieldDifference(b));
				
		// generate two rename reps
		b = getStandard();
		b.removeProperty("foo");
		b.removeProperty("bar");
		b.addNamedType("faa", Integer.class);
		b.addNamedType("bra", String.class);
		expectMetdataException(a, b);
		expectMetdataException(b, a);
		assertNull(b.getFieldDifference(b));
		
		// generate rename rep and change type rep
		b = getStandard();
		b.removeProperty("foo");
		b.removeProperty("bar");
		b.addNamedType("faa", Integer.class);
		b.addNamedType("bar", Short.class);
		expectMetdataException(a, b);
		expectMetdataException(b, a);
		assertNull(b.getFieldDifference(b));
		
		// generate two change type reps
		b = getStandard();
		b.removeProperty("foo");
		b.removeProperty("bar");
		b.addNamedType("foo", String.class);
		b.addNamedType("bar", Integer.class);
		expectMetdataException(a, b);
		expectMetdataException(b, a);
		assertNull(b.getFieldDifference(b));
		
		
		// generate rep that changes both type and name of one prop
		b = getStandard();
		b.removeProperty("foo");
		b.addNamedType("faa", String.class);
		expectMetdataException(a, b);
		expectMetdataException(b, a);
		assertNull(b.getFieldDifference(b));
	}
	
	//helper method to get a standard testing object
	private ObjectRepresentation getStandard()
	{
		ObjectRepresentation a = new ObjectRepresentation()
		{
		};
		a.addNamedType("foo",  Integer.class);
		a.addNamedType("bar",  String.class);
		a.addNamedType("baz",  Double.class);
		return a;
		
	}
	
	private void expectMetdataException(ObjectRepresentation a, ObjectRepresentation b) 
	{
		boolean thrown = false;
		try
		{
			a.getFieldDifference(b);
		}
		catch (MetadataException e)
		{
			thrown = true;
		}
		assertTrue("Exception was not thrown",thrown);
	}

}
