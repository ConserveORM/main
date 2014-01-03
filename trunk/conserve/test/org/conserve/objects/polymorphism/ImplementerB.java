package org.conserve.objects.polymorphism;

import org.conserve.objects.SubInterface;

/**
 * @author Erik Berglund
 *
 */
public class ImplementerB implements SubInterface
{
	private static final long serialVersionUID = -137024254160693215L;
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
