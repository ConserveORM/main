package org.conserve.adapter;

import org.conserve.Persist;
import org.conserve.tools.Defaults;

/**
 * Adapter for Maria databases. Specifies behaviour and dialects specific to the
 * Maria database engine.
 * @author Erik Berglund
 *
 */
public class MariaAdapter extends AdapterBase
{

	/**
	 * @param persist
	 */
	public MariaAdapter(Persist persist)
	{
		super(persist);
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getVarCharKeyword()
	 */
	@Override
	public String getVarCharKeyword()
	{
		return "LONGTEXT";
	}

	@Override
	public String getVarCharKeyword(int length)
	{
		return getVarCharKeyword();
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getKeyLength()
	 */
	@Override
	public String getKeyLength()
	{
		// maximum allowed key length.
		return "(900)";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getIdentity()
	 */
	@Override
	public String getIdentity()
	{
		return "BIGINT AUTO_INCREMENT";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getLastInsertedIdentity(String)
	 */
	@Override
	public String getLastInsertedIdentity(String tableName)
	{
		return "SELECT LAST_INSERT_ID()";
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#isValidColumnName(java.lang.String)
	 */
	@Override
	public boolean isValidColumnName(String name)
	{
		if (name.equalsIgnoreCase("KEY"))
		{
			return false;
		}
		else
		{
			return super.isValidColumnName(name);
		}
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#isSupportsClob()
	 */
	@Override
	public boolean isSupportsClob()
	{
		return false;
	}

	/**
	 * @see org.conserve.adapter.AdapterBase#getRenameColumnStatement()
	 */
	@Override
	public String getRenameColumnStatement()
	{
		StringBuilder statement = new StringBuilder("ALTER TABLE ");
		statement.append(Defaults.TABLENAME_PLACEHOLDER);
		statement.append(" CHANGE ");
		statement.append(Defaults.OLD_COLUMN_NAME_PLACEHOLDER);
		statement.append(" ");
		statement.append(Defaults.NEW_COLUMN_NAME_PLACEHOLDER);
		statement.append(" ");
		statement.append(Defaults.NEW_COLUMN_DESCRIPTION_PLACEHOLDER);
		return statement.toString();
	}
}
