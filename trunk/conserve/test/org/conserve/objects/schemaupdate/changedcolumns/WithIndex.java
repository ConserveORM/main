package org.conserve.objects.schemaupdate.changedcolumns;

import org.conserve.annotations.Indexed;
import org.conserve.annotations.MultiIndexed;

/**
 * A class with a column with an index.
 * 
 * @author Erik Berglund
 *
 */
public class WithIndex
{
	private String value;
	private String otherValue;

	/**
	 * @return the value
	 */
	@Indexed("fooidx")
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
	@MultiIndexed(values={"fooidx","baridx"})
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
