package org.conserve.objects;

/**
 * An object with property names that map to banned column names.
 * @author Erik Berglund
 *
 */
public class BadColumnNames
{
	private long count;
	private String key;
	public long getCount()
	{
		return count;
	}
	public void setCount(long count)
	{
		this.count = count;
	}
	public String getKey()
	{
		return key;
	}
	public void setKey(String key)
	{
		this.key = key;
	}
}
