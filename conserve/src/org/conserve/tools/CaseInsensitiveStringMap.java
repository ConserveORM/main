package org.conserve.tools;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * An implementation of Map that only stores Strings and only uses Strings as
 * keys. The map is case insensitive. Example:
 * 
 * CaseInsensitiveStringMap map = new... 
 * 
 * map.put("foo","bar");
 * if(map.containsKey("FOO")) { //this will be executed }
 * 
 * @author Erik Berglund
 *
 */
public class CaseInsensitiveStringMap implements Map<String, String>
{
	private Set<Map.Entry<String, String>> entries = new HashSet<>();

	/**
	 * @see java.util.Map#clear()
	 */
	@Override
	public void clear()
	{
		entries.clear();

	}

	/**
	 * @see java.util.Map#containsKey(java.lang.Object)
	 */
	@Override
	public boolean containsKey(Object keyObject)
	{
		String key = (String) keyObject;
		boolean res = false;
		for (Map.Entry<String, String> e : entries)
		{
			if (key == null)
			{
				if (e.getKey() == null)
				{
					res = true;
					break;
				}
			}
			else if (key.equalsIgnoreCase(e.getKey()))
			{
				res = true;
				break;
			}
		}
		return res;
	}

	/**
	 * @see java.util.Map#containsValue(java.lang.Object)
	 */
	@Override
	public boolean containsValue(Object value)
	{
		String key = (String) value;
		boolean res = false;
		for (Map.Entry<String, String> e : entries)
		{
			if (key == null)
			{
				if (e.getValue() == null)
				{
					res = true;
					break;
				}
			}
			else if (key.equalsIgnoreCase(e.getValue()))
			{
				res = true;
				break;
			}
		}
		return res;
	}


	/**
	 * @see java.util.Map#entrySet()
	 */
	@Override
	public Set<Map.Entry<String, String>> entrySet()
	{
		return entries;
	}

	/**
	 * @see java.util.Map#get(java.lang.Object)
	 */
	@Override
	public String get(Object keyObject)
	{
		String key = (String) keyObject;
		for (Map.Entry<String, String> e : entries)
		{
			if (key == null)
			{
				if (e.getKey() == null)
				{
					return e.getValue();
				}
			}
			else if (key.equalsIgnoreCase(e.getKey()))
			{
				return e.getValue();
			}
		}
		return null;
	}

	/**
	 * @see java.util.Map#isEmpty()
	 */
	@Override
	public boolean isEmpty()
	{
		return entries.isEmpty();
	}

	/**
	 * @see java.util.Map#keySet()
	 */
	@Override
	public Set<String> keySet()
	{
		throw new NotImplementedException();
	}

	/**
	 * @see java.util.Map#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public String put(String key, String value)
	{
		remove(key);
		entries.add(new AbstractMap.SimpleEntry<>(key, value));
		return value;
	}

	/**
	 * @see java.util.Map#putAll(java.util.Map)
	 */
	@Override
	public void putAll(Map<? extends String, ? extends String> m)
	{
		Set<? extends String> nuKeys = m.keySet();
		for (String s : nuKeys)
		{
			String value = m.get(s);
			put(s, value);
		}
	}

	/**
	 * @see java.util.Map#remove(java.lang.Object)
	 */
	@Override
	public String remove(Object keyObject)
	{
		String key = (String) keyObject;
		Map.Entry<String,String> toRemove = null;
		for (Map.Entry<String, String> e : entries)
		{
			if (key == null)
			{
				if (e.getKey() == null)
				{
					toRemove = e;
					break;
				}
			}
			else if (key.equalsIgnoreCase(e.getKey()))
			{
				toRemove = e;
				break;
			}
		}
		if(toRemove != null)
		{
			entries.remove(toRemove);
			return toRemove.getValue();
		}
		return null;
	}

	/**
	 * @see java.util.Map#size()
	 */
	@Override
	public int size()
	{
		return entries.size();
	}

	/**
	 * @see java.util.Map#values()
	 */
	@Override
	public Collection<String> values()
	{
		throw new NotImplementedException();
	}
}
