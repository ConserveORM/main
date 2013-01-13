package org.conserve.tools.generators;

/**
 * Describes a database field with talbe name and short table name.
 * The short name is only valid within a given query construction context and should not be reused.
 * 
 * 
 * @author Erik Berglund
 *
 */
public class FieldDescriptor
{
	private String fieldName;
	private String shortName;
	private String tableName;

	public FieldDescriptor(String tableName, String shortName, String fieldName)
	{
		this.tableName=tableName;
		this.shortName = shortName;
		this.fieldName=fieldName;
	}
	
	public String toShortString()
	{
		return shortName+"."+fieldName;
	}
	public String toFullString()
	{
		return fieldName+"."+fieldName;
	}

	/**
	 * @return the fieldName
	 */
	public String getFieldName()
	{
		return fieldName;
	}

	/**
	 * @return the shortName
	 */
	public String getShortName()
	{
		return shortName;
	}

	/**
	 * @return the tableName
	 */
	public String getTableName()
	{
		return tableName;
	}
}
