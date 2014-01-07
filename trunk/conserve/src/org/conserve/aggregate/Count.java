package org.conserve.aggregate;


/**
 * @author Erik Berglund
 * 
 */
public class Count extends AggregateFunction
{

	/**
	 * Default constructor that generates a count of number of rows returned,
	 * not a specific field.
	 */
	public Count()
	{
		super(null);
	}

	/**
	 * @param methodName
	 */
	public Count(String methodName)
	{
		super(methodName);
	}

	/**
	 * @see org.conserve.aggregate.AggregateFunction#translateReturnType(java.lang.Class)
	 */
	@Override
	protected Class<?> translateReturnType(Class<?> clazz)
	{
		//COUNT always returns Long
		return Long.class;
	}

	/**
	 * @see org.conserve.aggregate.AggregateFunction#getFunctionName()
	 */
	@Override
	protected String getFunctionName()
	{
		return "COUNT";
	}
}
