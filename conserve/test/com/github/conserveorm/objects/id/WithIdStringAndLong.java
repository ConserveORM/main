package com.github.conserveorm.objects.id;

import com.github.conserveorm.annotations.Id;

/**
 * @author Erik Berglund
 *
 */
public class WithIdStringAndLong
{
	private String theStringId;
	private Long theLongId;
	private String someOtherString;
	private Float value;
	
	@Id
	public String getTheStringId()
	{
		return theStringId;
	}
	public void setTheStringId(String theStringId)
	{
		this.theStringId = theStringId;
	}
	@Id
	public Long getTheLongId()
	{
		return theLongId;
	}
	public void setTheLongId(Long theLongId)
	{
		this.theLongId = theLongId;
	}
	public String getSomeOtherString()
	{
		return someOtherString;
	}
	public void setSomeOtherString(String someOtherString)
	{
		this.someOtherString = someOtherString;
	}
	public Float getValue()
	{
		return value;
	}
	public void setValue(Float value)
	{
		this.value = value;
	}
	
}
