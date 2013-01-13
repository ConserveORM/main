package org.conserve.objects.schemaupdate;

/**
 * Just a sub-class of OriginalObject
 * @author Erik Berglund
 *
 */
public class SubClass extends OriginalObject
{
	private int value;

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
}
