package org.conserve.aggregate;

/**
 * AggregateFunction subclass that implements the SQL SUM(...) function.
 * 
 * @author Erik Berglund
 *
 */
public class Sum extends AggregateFunction
{

	/**
	 * @param fieldName
	 */
	public Sum(String fieldName)
	{
		super(fieldName);
	}

	/**
	 * @see org.conserve.aggregate.AggregateFunction#getFunctionName()
	 */
	@Override
	protected String getFunctionName()
	{
		return "SUM";
	}

}
