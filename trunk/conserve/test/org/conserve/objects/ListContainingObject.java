package org.conserve.objects;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Erik Berglund
 * 
 */
public class ListContainingObject
{
	private String name;
	private List<String> list = new ArrayList<String>();

	@SuppressWarnings("unused")
	private void setList(List<String> list)
	{
		this.list = list;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public void addStr(String str)
	{
		list.add(str);
	}

	public List<String> getList()
	{
		return list;
	}
}
