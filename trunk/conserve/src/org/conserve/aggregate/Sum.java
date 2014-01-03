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

	/**
	 * @see org.conserve.aggregate.AggregateFunction#translateReturnType(java.lang.Class)
	 */
	@Override
	protected Class<?> translateReturnType(Class<?> clazz)
	{
		Class<?>res = null;
		// all integer type results are widened to Long
		if (clazz == Long.class || clazz == Integer.class || clazz == Short.class
				|| clazz == Byte.class || clazz == long.class || clazz == int.class || clazz == short.class || clazz == byte.class)
		{
			res = Long.class;
		}
		//all floating point results are widened to Double
		else if (clazz == Double.class || clazz == Float.class || clazz == double.class || clazz == float.class)
		{
			res = Double.class;
		}
		return res;
	}
}
