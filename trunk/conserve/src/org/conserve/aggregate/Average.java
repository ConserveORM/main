package org.conserve.aggregate;

import java.lang.reflect.Method;

import org.conserve.tools.NameGenerator;
import org.conserve.tools.metadata.ObjectRepresentation;
import org.conserve.tools.metadata.ObjectStack;

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
	 * Override the default implementation so that we can add a cast to double in the query.
	 * 
	 * This causes the data type to be widened and/or cast to floating point for the maximum accuracy under Java.
	 * 
	 * Get the SQL string that represents the aggregate function on a field.
	 * @param stack the stack representing the type to query.
	 * @return a String that can be used as part of an SQL select statement.
	 * @throws SecurityException 
	 * @throws NoSuchMethodException 
	 */
	@Override
	public String getStringRepresentation(ObjectStack stack) throws NoSuchMethodException, SecurityException
	{
		Method method = stack.getActualRepresentation().getRepresentedClass().getMethod(methodName);
		String colname = NameGenerator.getColumnName(method);
		ObjectRepresentation rep = stack.getRepresentation(colname);
		
		StringBuilder res = new StringBuilder(getFunctionName());
		res.append("(CAST(");
		res.append(rep.getAsName());
		res.append(".");
		res.append(colname);
		res.append(" AS DOUBLE))");
		return res.toString();
	}

	/**
	 * @see org.conserve.aggregate.AggregateFunction#getFunctionName()
	 */
	@Override
	protected String getFunctionName()
	{
		return "AVG";
	}

	/**
	 * @see org.conserve.aggregate.AggregateFunction#translateReturnType(java.lang.Class)
	 */
	@Override
	protected Class<? extends Number> translateReturnType(Class<?> clazz)
	{
		//averages are always Double
		return Double.class;
	}

}
