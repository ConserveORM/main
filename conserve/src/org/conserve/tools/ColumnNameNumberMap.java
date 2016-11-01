package org.conserve.tools;

import org.conserve.adapter.AdapterBase;

/**
 * Maps between column names and reference numbers.
 * @author Erik Berglund
 *
 */
public class ColumnNameNumberMap extends NameNumberMap
{

	/**
	 * @param adapter
	 * @param tableName
	 */
	public ColumnNameNumberMap(AdapterBase adapter)
	{
		super(adapter, Defaults.COLUMN_NAME_MAP_TABLE);
	}

}
