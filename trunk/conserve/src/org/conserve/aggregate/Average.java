package org.conserve.aggregate;

/**
 * Subclass of AggregateFunction that represents the SQL AVG(...) function.
 * 
 * @author Erik Berglund
 *
 */
public class Average extends AggregateFunction
{

	/**
	 * @param fieldName
	 */
	public Average(String fieldName)
	{
		super(fieldName);
	}

	/**
	 * @see org.conserve.aggregate.AggregateFunction#getFunctionName()
	 */
	@Override
	protected String getFunctionName()
	{
		return "AVG";
	}

}
