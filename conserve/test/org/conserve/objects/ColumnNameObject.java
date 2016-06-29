/**
 * 
 */
package org.conserve.objects;

import org.conserve.annotations.ColumnName;

/**
 * 
 * Object with no other function than having a column named by annotation.
 * 
 * @author Erik Berglund
 *
 */
public class ColumnNameObject
{
	private String name;

	@ColumnName("alternativename")
	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}
	
}
