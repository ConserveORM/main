package org.conserve.objects.polymorphism;

import org.conserve.objects.SubInterface;

/**
 * @author Erik Berglund
 *
 */
public class ImplementerA implements SubInterface
{
	private static final long serialVersionUID = -5436610031137796305L;
	private Integer someValue;
	
	public void setSomeValue(Integer v)
	{
		this.someValue = v;
	}

	/**
	 * @see org.conserve.objects.SubInterface#getSomeValue()
	 */
	@Override
	public Integer getSomeValue()
	{
		return someValue;
	}

}
