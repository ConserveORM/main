package org.conserve.aggregate;

import java.lang.reflect.Method;

import org.conserve.tools.NameGenerator;
import org.conserve.tools.metadata.ObjectRepresentation;
import org.conserve.tools.metadata.ObjectStack;

/**
 * Class that represent one of the SQL aggregate functions.
 * 
 * @author Erik Berglund
 *
 */
public abstract class AggregateFunction
{
	protected String fieldName;
	
	/**
	 * Constructor  that names takes name of field to aggregate over, e.g. "getFoo", "getBar".
	 * 
	 * @param fieldName the name of the function to aggregate over.
	 */
	public AggregateFunction(String fieldName)
	{
		this.fieldName = fieldName;
	}
	
	
	/**
	 * Get the SQL string that represents the aggregate function on a field.
	 * @param stack the stack representing the type to query.
	 * @return a String that can be used as part of an SQL select statement.
	 * @throws SecurityException 
	 * @throws NoSuchMethodException 
	 */
	public String getStringRepresentation(ObjectStack stack) throws NoSuchMethodException, SecurityException
	{
		Method method = stack.getActualRepresentation().getRepresentedClass().getMethod(fieldName);
		String colname = NameGenerator.getColumnName(method);
		ObjectRepresentation rep = stack.getRepresentation(colname);
		
		StringBuilder res = new StringBuilder(getFunctionName());
		res.append("(");
		res.append(rep.getAsName());
		res.append(".");
		res.append(colname);
		res.append(")");
		return res.toString();
	}
	
	/**
	 * Get the name of the SQL aggregate function this class represents.
	 * @return
	 */
	protected abstract String getFunctionName();
}
