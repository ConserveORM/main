package org.conserve.tools.metadata;

/**
 * 
 * This exception indicates that something went wrong with the object model meta data.
 * 
 * @author Erik Berglund
 *
 */
public class MetadataException extends Exception
{
	private static final long serialVersionUID = 7198084710327837209L;
	
	/**
	 * @param string
	 */
	public MetadataException(String string)
	{
		super(string);
	}
	
}
