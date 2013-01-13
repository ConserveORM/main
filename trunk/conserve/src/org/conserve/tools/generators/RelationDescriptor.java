package org.conserve.tools.generators;

/**
 * @author Erik Berglund
 *
 */
public class RelationDescriptor
{
	private boolean requiresvalue;
	
	private FieldDescriptor first;
	private FieldDescriptor second;
	private Object value;

	public RelationDescriptor(FieldDescriptor first,FieldDescriptor second)
	{
		this.first = first;
		this.second = second;
	}
	public RelationDescriptor(FieldDescriptor first,Object value)
	{
		this.first = first;
		this.value = value;
		this.requiresvalue = true;
	}
	/**
	 * @return the requiresvalue
	 */
	public boolean isRequiresvalue()
	{
		return requiresvalue;
	}
	/**
	 * @return the first
	 */
	public FieldDescriptor getFirst()
	{
		return first;
	}
	/**
	 * @return the second
	 */
	public FieldDescriptor getSecond()
	{
		return second;
	}
	/**
	 * @return the value
	 */
	public Object getValue()
	{
		return value;
	}
	
	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder(first.toShortString());
		sb.append(" = ");
		if(isRequiresvalue())
		{
			sb.append("?");
		}
		else
		{
			sb.append(second.toShortString());
		}
		return sb.toString();
	}
}
