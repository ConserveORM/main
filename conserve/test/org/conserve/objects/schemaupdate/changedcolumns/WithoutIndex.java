package org.conserve.objects.schemaupdate.changedcolumns;

/**
 * Same as WithIndex, but without index.
 * @author Erik Berglund
 *
 */
public class WithoutIndex
{
	private String value;
	private String otherValue;

	/**
	 * @return the value
	 */
	public String getValue()
	{
		return value;
	}

	/**
	 * @param value the value to set
	 */
	public void setValue(String value)
	{
		this.value = value;
	}

	/**
	 * @return the otherValue
	 */
	public String getOtherValue()
	{
		return otherValue;
	}

	/**
	 * @param otherValue the otherValue to set
	 */
	public void setOtherValue(String otherValue)
	{
		this.otherValue = otherValue;
	}

}
