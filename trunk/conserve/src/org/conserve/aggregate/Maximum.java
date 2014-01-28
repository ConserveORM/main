package org.conserve.aggregate;

/**
 * Subclass of AggregateFunction that represents the SQL MAX(...) function.
 * 
 * @author Erik Berglund
 *
 */
public class Maximum extends AggregateFunction
{

	/**
	 * @param fieldName
	 */
	public Maximum(String fieldName)
	{
		super(fieldName);
	}

	/**
	 * @see org.conserve.aggregate.AggregateFunction#getFunctionName()
	 */
	@Override
	protected String getFunctionName()
	{
		return "MAX";
	}

	/**
	 * @see org.conserve.aggregate.AggregateFunction#translateReturnType(java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected Class<? extends Number> translateReturnType(Class<?> clazz)
	{
		//no widening used for Maximum
		return (Class<? extends Number>) clazz;
	}
}
