package com.github.conserveorm.objects.id;

import com.github.conserveorm.annotations.Id;

/**
 * @author Erik Berglund
 *
 */
public class WithIdLong
{
	private Long theLongId;
	private String someOtherString;
	private Float value;

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
