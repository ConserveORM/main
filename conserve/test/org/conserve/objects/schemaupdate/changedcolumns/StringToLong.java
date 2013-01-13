package org.conserve.objects.schemaupdate.changedcolumns;

/**
 * Same as OriginalObject, but the String has been exchanged for a long.
 * 
 * @author Erik Berglund
 *
 */
public class StringToLong
{
	private long name;
	private int value;
	private Object otherObject;
	private Object redundantObject;
	private long[] array;
	
	/**
	 * @return the name
	 */
	public long getName()
	{
		return name;
	}
	/**
	 * @param name the name to set
	 */
	public void setName(long name)
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
	public Object getOtherObject()
	{
		return otherObject;
	}
	/**
	 * @param otherObject the otherObject to set
	 */
	public void setOtherObject(Object otherObject)
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