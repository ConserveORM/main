package org.conserve.tools;

import static org.junit.Assert.*;

import java.sql.Timestamp;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Time;
import java.sql.Date;

import org.conserve.objects.ComplexObject;
import org.conserve.objects.MyEnum;
import org.conserve.objects.MyOtherEnum;
import org.conserve.objects.SimpleObject;
import org.conserve.objects.SimplestObject;
import org.conserve.objects.schemaupdate.copydown.AfterBottom;
import org.conserve.objects.schemaupdate.copydown.BeforeBottom;
import org.conserve.objects.schemaupdate.copydown.BeforeTop;
import org.conserve.objects.schemaupdate.copydown.OriginalBottom;
import org.conserve.objects.schemaupdate.copydown.OriginalMiddle;
import org.conserve.objects.schemaupdate.copydown.OriginalTop;
import org.junit.Test;

/**
 * @author Erik Berglund
 *
 */
public class CompabilityCalculatorTest
{

	/**
	 * Test method for {@link org.conserve.tools.CompabilityCalculator#calculate(java.lang.Class, java.lang.Class)}.
	 */
	@Test
	public void testCalculate()
	{
		//converting from boolean to all possibilities
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Boolean.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Character.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Short.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Integer.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Long.class));
		assertFalse(CompabilityCalculator.calculate(Boolean.class, Double.class));
		assertFalse(CompabilityCalculator.calculate(Boolean.class, double.class));
		assertFalse(CompabilityCalculator.calculate(Boolean.class, Object.class));

		
		//converting from byte to all possibilities
		assertFalse(CompabilityCalculator.calculate(Byte.class, Boolean.class));
		assertTrue(CompabilityCalculator.calculate(Byte.class, Byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Character.class));
		assertTrue(CompabilityCalculator.calculate(Byte.class, Short.class));
		assertTrue(CompabilityCalculator.calculate(Byte.class, Integer.class));
		assertTrue(CompabilityCalculator.calculate(Byte.class, Long.class));
		assertFalse(CompabilityCalculator.calculate(Byte.class, Double.class));
		assertFalse(CompabilityCalculator.calculate(Byte.class, double.class));
		assertFalse(CompabilityCalculator.calculate(Byte.class, Object.class));
		
		//converting from char to all possibilities
		assertFalse(CompabilityCalculator.calculate(Character.class, Boolean.class));
		assertFalse(CompabilityCalculator.calculate(Character.class, Byte.class));
		assertTrue(CompabilityCalculator.calculate(Character.class, Character.class));
		assertTrue(CompabilityCalculator.calculate(Character.class, Short.class));
		assertTrue(CompabilityCalculator.calculate(Character.class, Integer.class));
		assertTrue(CompabilityCalculator.calculate(Character.class, Long.class));
		assertFalse(CompabilityCalculator.calculate(Character.class, Double.class));
		assertFalse(CompabilityCalculator.calculate(Character.class, double.class));
		assertFalse(CompabilityCalculator.calculate(Character.class, Object.class));

		assertFalse(CompabilityCalculator.calculate(char.class, Boolean.class));
		assertFalse(CompabilityCalculator.calculate(char.class, Byte.class));
		assertTrue(CompabilityCalculator.calculate(char.class, Character.class));
		assertTrue(CompabilityCalculator.calculate(char.class, Short.class));
		assertTrue(CompabilityCalculator.calculate(char.class, Integer.class));
		assertTrue(CompabilityCalculator.calculate(char.class, Long.class));
		assertFalse(CompabilityCalculator.calculate(char.class, Double.class));
		assertFalse(CompabilityCalculator.calculate(char.class, double.class));
		assertFalse(CompabilityCalculator.calculate(char.class, Object.class));
		
		
		//converting from short to all possibilities
		assertFalse(CompabilityCalculator.calculate(Short.class, Boolean.class));
		assertFalse(CompabilityCalculator.calculate(Short.class, Byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Character.class));
		assertTrue(CompabilityCalculator.calculate(Short.class, Short.class));
		assertTrue(CompabilityCalculator.calculate(Short.class, Integer.class));
		assertTrue(CompabilityCalculator.calculate(Short.class, Long.class));
		assertFalse(CompabilityCalculator.calculate(Short.class, Double.class));
		assertFalse(CompabilityCalculator.calculate(Short.class, double.class));
		assertFalse(CompabilityCalculator.calculate(Short.class, Object.class));
		
		
		//converting from int to all possibilities
		assertFalse(CompabilityCalculator.calculate(Integer.class, Boolean.class));
		assertFalse(CompabilityCalculator.calculate(Integer.class, Byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Character.class));
		assertFalse(CompabilityCalculator.calculate(Integer.class, Short.class));
		assertTrue(CompabilityCalculator.calculate(Integer.class, Integer.class));
		assertTrue(CompabilityCalculator.calculate(Integer.class, Long.class));
		assertFalse(CompabilityCalculator.calculate(Integer.class, Double.class));
		assertFalse(CompabilityCalculator.calculate(Integer.class, double.class));
		assertFalse(CompabilityCalculator.calculate(Integer.class, Object.class));
		
		assertFalse(CompabilityCalculator.calculate(int.class, Boolean.class));
		assertFalse(CompabilityCalculator.calculate(int.class, Byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Character.class));
		assertFalse(CompabilityCalculator.calculate(int.class, Short.class));
		assertTrue(CompabilityCalculator.calculate(int.class, Integer.class));
		assertTrue(CompabilityCalculator.calculate(int.class, Long.class));
		assertFalse(CompabilityCalculator.calculate(int.class, Double.class));
		assertFalse(CompabilityCalculator.calculate(int.class, double.class));
		assertFalse(CompabilityCalculator.calculate(int.class, Object.class));
		
		assertFalse(CompabilityCalculator.calculate(Integer.class, boolean.class));
		assertFalse(CompabilityCalculator.calculate(Integer.class, byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, char.class));
		assertFalse(CompabilityCalculator.calculate(Integer.class, short.class));
		assertTrue(CompabilityCalculator.calculate(Integer.class, int.class));
		assertTrue(CompabilityCalculator.calculate(Integer.class, long.class));
		assertFalse(CompabilityCalculator.calculate(Integer.class, double.class));
		assertFalse(CompabilityCalculator.calculate(Integer.class, double.class));
		
		assertFalse(CompabilityCalculator.calculate(int.class, boolean.class));
		assertFalse(CompabilityCalculator.calculate(int.class, byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, char.class));
		assertFalse(CompabilityCalculator.calculate(int.class, short.class));
		assertTrue(CompabilityCalculator.calculate(int.class, int.class));
		assertTrue(CompabilityCalculator.calculate(int.class, long.class));
		assertFalse(CompabilityCalculator.calculate(int.class, double.class));
		assertFalse(CompabilityCalculator.calculate(int.class, double.class));
		

		//converting from long to all possibilities
		assertFalse(CompabilityCalculator.calculate(Long.class, Boolean.class));
		assertFalse(CompabilityCalculator.calculate(Long.class, Byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Character.class));
		assertFalse(CompabilityCalculator.calculate(Long.class, Short.class));
		assertFalse(CompabilityCalculator.calculate(Long.class, Integer.class));
		assertTrue(CompabilityCalculator.calculate(Long.class, Long.class));
		assertFalse(CompabilityCalculator.calculate(Long.class, Double.class));
		assertFalse(CompabilityCalculator.calculate(Long.class, double.class));
		assertFalse(CompabilityCalculator.calculate(Long.class, Object.class));

		assertFalse(CompabilityCalculator.calculate(long.class, Boolean.class));
		assertFalse(CompabilityCalculator.calculate(long.class, Byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Character.class));
		assertFalse(CompabilityCalculator.calculate(long.class, Short.class));
		assertFalse(CompabilityCalculator.calculate(long.class, Integer.class));
		assertTrue(CompabilityCalculator.calculate(long.class, Long.class));
		assertFalse(CompabilityCalculator.calculate(long.class, Double.class));
		assertFalse(CompabilityCalculator.calculate(long.class, double.class));
		assertFalse(CompabilityCalculator.calculate(long.class, Object.class));

		assertFalse(CompabilityCalculator.calculate(Long.class, boolean.class));
		assertFalse(CompabilityCalculator.calculate(Long.class, byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, char.class));
		assertFalse(CompabilityCalculator.calculate(Long.class, short.class));
		assertFalse(CompabilityCalculator.calculate(Long.class, int.class));
		assertTrue(CompabilityCalculator.calculate(Long.class, long.class));
		assertFalse(CompabilityCalculator.calculate(Long.class, double.class));
		assertFalse(CompabilityCalculator.calculate(Long.class, double.class));

		assertFalse(CompabilityCalculator.calculate(long.class, boolean.class));
		assertFalse(CompabilityCalculator.calculate(long.class, byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, char.class));
		assertFalse(CompabilityCalculator.calculate(long.class, short.class));
		assertFalse(CompabilityCalculator.calculate(long.class, int.class));
		assertTrue(CompabilityCalculator.calculate(long.class, long.class));
		assertFalse(CompabilityCalculator.calculate(long.class, double.class));
		assertFalse(CompabilityCalculator.calculate(long.class, double.class));
		

		//converting from double to all possibilities
		assertFalse(CompabilityCalculator.calculate(double.class, Boolean.class));
		assertFalse(CompabilityCalculator.calculate(double.class, Byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Character.class));
		assertFalse(CompabilityCalculator.calculate(double.class, Short.class));
		assertFalse(CompabilityCalculator.calculate(double.class, Integer.class));
		assertFalse(CompabilityCalculator.calculate(double.class, Long.class));
		assertTrue(CompabilityCalculator.calculate(double.class, Double.class));
		assertTrue(CompabilityCalculator.calculate(double.class, double.class));
		assertFalse(CompabilityCalculator.calculate(double.class, Object.class));

		assertFalse(CompabilityCalculator.calculate(double.class, Boolean.class));
		assertFalse(CompabilityCalculator.calculate(double.class, Byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Character.class));
		assertFalse(CompabilityCalculator.calculate(double.class, Short.class));
		assertFalse(CompabilityCalculator.calculate(double.class, Integer.class));
		assertFalse(CompabilityCalculator.calculate(double.class, Long.class));
		assertTrue(CompabilityCalculator.calculate(double.class, Double.class));
		assertTrue(CompabilityCalculator.calculate(double.class, double.class));
		assertFalse(CompabilityCalculator.calculate(double.class, Object.class));

		assertFalse(CompabilityCalculator.calculate(double.class, boolean.class));
		assertFalse(CompabilityCalculator.calculate(double.class, byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, char.class));
		assertFalse(CompabilityCalculator.calculate(double.class, short.class));
		assertFalse(CompabilityCalculator.calculate(double.class, int.class));
		assertFalse(CompabilityCalculator.calculate(double.class, long.class));
		assertTrue(CompabilityCalculator.calculate(double.class, double.class));
		assertTrue(CompabilityCalculator.calculate(double.class, double.class));

		assertFalse(CompabilityCalculator.calculate(double.class, boolean.class));
		assertFalse(CompabilityCalculator.calculate(double.class, byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, char.class));
		assertFalse(CompabilityCalculator.calculate(double.class, short.class));
		assertFalse(CompabilityCalculator.calculate(double.class, int.class));
		assertFalse(CompabilityCalculator.calculate(double.class, long.class));
		assertTrue(CompabilityCalculator.calculate(double.class, double.class));
		assertTrue(CompabilityCalculator.calculate(double.class, double.class));
		
		

		//converting from double to all possibilities
		assertFalse(CompabilityCalculator.calculate(Double.class, Boolean.class));
		assertFalse(CompabilityCalculator.calculate(Double.class, Byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Character.class));
		assertFalse(CompabilityCalculator.calculate(Double.class, Short.class));
		assertFalse(CompabilityCalculator.calculate(Double.class, Integer.class));
		assertFalse(CompabilityCalculator.calculate(Double.class, Long.class));
		assertFalse(CompabilityCalculator.calculate(Double.class, Float.class));
		assertTrue(CompabilityCalculator.calculate(Double.class, Double.class));
		assertFalse(CompabilityCalculator.calculate(Double.class, Object.class));

		assertFalse(CompabilityCalculator.calculate(double.class, Boolean.class));
		assertFalse(CompabilityCalculator.calculate(double.class, Byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Character.class));
		assertFalse(CompabilityCalculator.calculate(double.class, Short.class));
		assertFalse(CompabilityCalculator.calculate(double.class, Integer.class));
		assertFalse(CompabilityCalculator.calculate(double.class, Long.class));
		assertFalse(CompabilityCalculator.calculate(double.class, Float.class));
		assertTrue(CompabilityCalculator.calculate(double.class, Double.class));
		assertFalse(CompabilityCalculator.calculate(double.class, Object.class));

		assertFalse(CompabilityCalculator.calculate(Double.class, boolean.class));
		assertFalse(CompabilityCalculator.calculate(Double.class, byte.class));
		assertFalse(CompabilityCalculator.calculate(Double.class, char.class));
		assertFalse(CompabilityCalculator.calculate(Double.class, short.class));
		assertFalse(CompabilityCalculator.calculate(Double.class, int.class));
		assertFalse(CompabilityCalculator.calculate(Double.class, long.class));
		assertFalse(CompabilityCalculator.calculate(Double.class, float.class));
		assertTrue(CompabilityCalculator.calculate(Double.class, double.class));

		assertFalse(CompabilityCalculator.calculate(double.class, boolean.class));
		assertFalse(CompabilityCalculator.calculate(double.class, byte.class));
		assertFalse(CompabilityCalculator.calculate(double.class, char.class));
		assertFalse(CompabilityCalculator.calculate(double.class, short.class));
		assertFalse(CompabilityCalculator.calculate(double.class, int.class));
		assertFalse(CompabilityCalculator.calculate(double.class, long.class));
		assertFalse(CompabilityCalculator.calculate(double.class, float.class));
		assertTrue(CompabilityCalculator.calculate(double.class, double.class));
		
		//converting from object to any of the primitive classes
		assertFalse(CompabilityCalculator.calculate(Object.class, Boolean.class));
		assertFalse(CompabilityCalculator.calculate(Object.class, Byte.class));
		assertTrue(CompabilityCalculator.calculate(Boolean.class, Character.class));
		assertFalse(CompabilityCalculator.calculate(Object.class, Short.class));
		assertFalse(CompabilityCalculator.calculate(Object.class, Integer.class));
		assertFalse(CompabilityCalculator.calculate(Object.class, Long.class));
		assertFalse(CompabilityCalculator.calculate(Object.class, Float.class));
		assertFalse(CompabilityCalculator.calculate(Object.class, Double.class));
		assertFalse(CompabilityCalculator.calculate(Object.class, boolean.class));
		assertFalse(CompabilityCalculator.calculate(Object.class, byte.class));
		assertFalse(CompabilityCalculator.calculate(Object.class, char.class));
		assertFalse(CompabilityCalculator.calculate(Object.class, short.class));
		assertFalse(CompabilityCalculator.calculate(Object.class, int.class));
		assertFalse(CompabilityCalculator.calculate(Object.class, long.class));
		assertFalse(CompabilityCalculator.calculate(Object.class, float.class));
		assertFalse(CompabilityCalculator.calculate(Object.class, double.class));
		assertTrue(CompabilityCalculator.calculate(Object.class, Object.class));
		
		//make sure object to same object is ok
		assertTrue(CompabilityCalculator.calculate(Object.class, Object.class));
		assertTrue(CompabilityCalculator.calculate(SimpleObject.class, SimpleObject.class));
		assertTrue(CompabilityCalculator.calculate(SimplestObject.class, SimplestObject.class));
		assertTrue(CompabilityCalculator.calculate(ComplexObject.class, ComplexObject.class));
		assertTrue(CompabilityCalculator.calculate(BeforeTop.class, BeforeTop.class));
		assertTrue(CompabilityCalculator.calculate(AfterBottom.class, AfterBottom.class));
		
		//make sure all directs are correctly converted
		assertFalse(CompabilityCalculator.calculate(Time.class,String.class));
		assertFalse(CompabilityCalculator.calculate(Time.class,Blob.class));
		assertFalse(CompabilityCalculator.calculate(Time.class,Clob.class));
		assertFalse(CompabilityCalculator.calculate(Time.class,Date.class));
		assertTrue(CompabilityCalculator.calculate(Time.class,Time.class));
		assertTrue(CompabilityCalculator.calculate(Time.class,Timestamp.class));
		assertFalse(CompabilityCalculator.calculate(Timestamp.class,String.class));
		assertFalse(CompabilityCalculator.calculate(Timestamp.class,Blob.class));
		assertFalse(CompabilityCalculator.calculate(Timestamp.class,Clob.class));
		assertFalse(CompabilityCalculator.calculate(Timestamp.class,Date.class));
		assertFalse(CompabilityCalculator.calculate(Timestamp.class,Time.class));
		assertTrue(CompabilityCalculator.calculate(Timestamp.class,Timestamp.class));
		assertFalse(CompabilityCalculator.calculate(Blob.class,String.class));
		assertTrue(CompabilityCalculator.calculate(Blob.class,Blob.class));
		assertFalse(CompabilityCalculator.calculate(Blob.class,Clob.class));
		assertFalse(CompabilityCalculator.calculate(Blob.class,Date.class));
		assertFalse(CompabilityCalculator.calculate(Blob.class,Time.class));
		assertFalse(CompabilityCalculator.calculate(Blob.class,Timestamp.class));
		assertFalse(CompabilityCalculator.calculate(Clob.class,String.class));
		assertFalse(CompabilityCalculator.calculate(Clob.class,Blob.class));
		assertTrue(CompabilityCalculator.calculate(Clob.class,Clob.class));
		assertFalse(CompabilityCalculator.calculate(Clob.class,Date.class));
		assertFalse(CompabilityCalculator.calculate(Clob.class,Time.class));
		assertFalse(CompabilityCalculator.calculate(Clob.class,Timestamp.class));
		assertFalse(CompabilityCalculator.calculate(Date.class,String.class));
		assertFalse(CompabilityCalculator.calculate(Date.class,Blob.class));
		assertFalse(CompabilityCalculator.calculate(Date.class,Clob.class));
		assertTrue(CompabilityCalculator.calculate(Date.class,Date.class));
		assertFalse(CompabilityCalculator.calculate(Date.class,Time.class));
		assertTrue(CompabilityCalculator.calculate(Date.class,Timestamp.class));
		assertTrue(CompabilityCalculator.calculate(String.class,String.class));
		assertFalse(CompabilityCalculator.calculate(String.class,Blob.class));
		assertFalse(CompabilityCalculator.calculate(String.class,Clob.class));
		assertFalse(CompabilityCalculator.calculate(String.class,Date.class));
		assertFalse(CompabilityCalculator.calculate(String.class,Time.class));
		assertFalse(CompabilityCalculator.calculate(String.class,Timestamp.class));

		assertTrue(CompabilityCalculator.calculate(int[].class, int[].class));
		assertFalse(CompabilityCalculator.calculate(int[].class, long[].class));
		assertFalse(CompabilityCalculator.calculate(long[].class, int[].class));

		assertTrue(CompabilityCalculator.calculate(MyEnum.class, String.class));
		assertTrue(CompabilityCalculator.calculate(MyEnum.class, MyEnum.class));
		assertTrue(CompabilityCalculator.calculate(MyEnum.class, MyOtherEnum.class));
		assertTrue(CompabilityCalculator.calculate(MyOtherEnum.class, MyEnum.class));
		assertFalse(CompabilityCalculator.calculate(String.class, MyEnum.class));
		assertFalse(CompabilityCalculator.calculate(Object.class, MyEnum.class));
		assertFalse(CompabilityCalculator.calculate(MyEnum.class, Object.class));
		
		//check sub and super classes
		assertTrue(CompabilityCalculator.calculate(OriginalTop.class, OriginalTop.class));
		assertTrue(CompabilityCalculator.calculate(OriginalMiddle.class, OriginalTop.class));
		assertTrue(CompabilityCalculator.calculate(OriginalBottom.class, OriginalTop.class));
		assertTrue(CompabilityCalculator.calculate(OriginalTop.class, OriginalMiddle.class));
		assertTrue(CompabilityCalculator.calculate(OriginalMiddle.class, OriginalMiddle.class));
		assertTrue(CompabilityCalculator.calculate(OriginalBottom.class, OriginalMiddle.class));
		assertTrue(CompabilityCalculator.calculate(OriginalTop.class, OriginalBottom.class));
		assertTrue(CompabilityCalculator.calculate(OriginalMiddle.class, OriginalBottom.class));
		assertTrue(CompabilityCalculator.calculate(OriginalBottom.class, OriginalBottom.class));
		
		//check some interfaces
		assertTrue(CompabilityCalculator.calculate(Cloneable.class, OriginalTop.class));
		assertTrue(CompabilityCalculator.calculate(OriginalTop.class, Cloneable.class));
		
	}

}
