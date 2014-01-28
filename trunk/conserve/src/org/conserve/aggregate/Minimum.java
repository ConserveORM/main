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
	@SuppressWarnings("unchecked")
	@Override
	protected Class<? extends Number> translateReturnType(Class<?> clazz)
	{
		//no widening used for Minimum
		return (Class<? extends Number>) clazz;
	}
}
