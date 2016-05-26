package org.conserve.adapter;

import org.conserve.Persist;

/**
 * Adapter for Maria databases. Specifies behaviour and dialects specific to the
 * Maria database engine.
 * @author Erik Berglund
 *
 */
public class MariaAdapter extends MySqlAdapter
{

	/**
	 * @param persist
	 */
	public MariaAdapter(Persist persist)
	{
		super(persist);
	}


}
