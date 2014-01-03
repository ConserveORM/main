package org.conserve.aggregate;

/**
 * Subclass of AggregateFunction that represents the SQL MIN(...) function.
 * 
 * @author Erik Berglund
 *
 */
public class Minimum extends AggregateFunction
{

	/**
	 * @param fieldName
	 */
	public Minimum(String fieldName)
	{
		super(fieldName);
	}

	/**
	 * @see org.conserve.aggregate.AggregateFunction#getFunctionName()
	 */
	@Override
	protected String getFunctionName()
	{
		return "MIN";
	}

	/**
	 * @see org.conserve.aggregate.AggregateFunction#translateReturnType(java.lang.Class)
	 */
	@Override
	protected Class<?> translateReturnType(Class<?> clazz)
	{
		//no widening used for Minimum
		return clazz;
	}
}
