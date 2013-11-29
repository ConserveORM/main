package org.conserve.objects.schemaupdate.changedcolumns;

import org.conserve.objects.SimplestObject;

/**
 * Same as OriginalObject, but the object has been exchanged for a SimplestObject.
 * 
 * @author Erik Berglund
 *
 */
public class ObjectToSubclass
{
	private String name;
	private int value;
	private SimplestObject otherObject;
	private Object redundantObject;
	private long[] array;
	
	/**
	 * @return the name
	 */
	public String getName()
	{
		return name;
	}
	/**
	 * @param name the name to set
	 */
	public void setName(String name)
	{
		this.name = name;
	}
	/**
	 * @return the value
	 */
	public int getValue()
	{
		return value;
	}
	/**
	 * @param value the value to set
	 */
	public void setValue(int value)
	{
		this.value = value;
	}
	/**
	 * @return the otherObject
	 */
	public SimplestObject getOtherObject()
	{
		return otherObject;
	}
	/**
	 * @param otherObject the otherObject to set
	 */
	public void setOtherObject(SimplestObject otherObject)
	{
		this.otherObject = otherObject;
	}
	/**
	 * @return the redundantObject
	 */
	public Object getRedundantObject()
	{
		return redundantObject;
	}
	/**
	 * @param redundantObject the redundantObject to set
	 */
	public void setRedundantObject(Object redundantObject)
	{
		this.redundantObject = redundantObject;
	}
	/**
	 * @return the array
	 */
	public long[] getArray()
	{
		return array;
	}
	/**
	 * @param array the array to set
	 */
	public void setArray(long[] array)
	{
		this.array = array;
	}
}
