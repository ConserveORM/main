package org.conserve.objects;

/**
 * Object that does nothing but contain a SimpleObject.
 * 
 * @author Erik Berglund
 *
 */
public class SimpleObjectContainer
{
	private SimpleObject simpleObject;

	/**
	 * @return the simpleObject
	 */
	public SimpleObject getSimpleObject()
	{
		return simpleObject;
	}

	/**
	 * @param simpleObject the simpleObject to set
	 */
	public void setSimpleObject(SimpleObject simpleObject)
	{
		this.simpleObject = simpleObject;
	}

}
