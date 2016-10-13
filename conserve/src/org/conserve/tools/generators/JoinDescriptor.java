package org.conserve.tools.generators;

/**
 * Describes a join statement between two tables.
 * 
 * 
 * @author Erik Berglund
 *
 */
public class JoinDescriptor
{
	private String leftTable;
	private String leftShortName;
	private String rightTable;
	private String onDescription;
	private Object [] values;
	
	public JoinDescriptor(String leftTable, String leftShortName,String rightTable, String onDescription, Object ... values)
	{
		this.leftTable = leftTable;
		this.leftShortName = leftShortName;
		this.rightTable = rightTable;
		this.onDescription = onDescription;
		this.values = values;
	}
	
	/**
	 * Check if this join description matches a given table and shortname.
	 * @param table
	 * @param shortName
	 * @return
	 */
	public boolean leftMatches(String table, String shortName)
	{
		return (leftTable.equalsIgnoreCase(table) && leftShortName.equalsIgnoreCase(shortName));
	}
	
	/**
	 * Get the values to be inserted in the statement in place of '?' markers.
	 * @return
	 */
	public Object [] getValues()
	{
		return values;
	}
	
	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return leftTable+" AS "+ leftShortName + " LEFT JOIN " + rightTable +" ON " + onDescription;
	}

}
