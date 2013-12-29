package org.conserve.objects.polymorphism;

import org.conserve.objects.SubInterface;

/**
 * @author Erik Berglund
 *
 */
public class ImplementerA implements SubInterface
{

	private int someValue;
	
	public void setSomeValue(int v)
	{
		this.someValue = v;
	}

	/**
	 * @see org.conserve.objects.SubInterface#getSomeValue()
	 */
	@Override
	public int getSomeValue()
	{
		return someValue;
	}

}
