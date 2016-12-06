/*******************************************************************************
 * Copyright (c) 2009, 2016 Erik Berglund.
 *    
 *        This file is part of Conserve.
 *    
 *        Conserve is free software: you can redistribute it and/or modify
 *        it under the terms of the GNU Affero General Public License as published by
 *        the Free Software Foundation, either version 3 of the License, or
 *        (at your option) any later version.
 *    
 *        Conserve is distributed in the hope that it will be useful,
 *        but WITHOUT ANY WARRANTY; without even the implied warranty of
 *        MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *        GNU Affero General Public License for more details.
 *    
 *        You should have received a copy of the GNU Affero General Public License
 *        along with Conserve.  If not, see <https://www.gnu.org/licenses/agpl.html>.
 *******************************************************************************/
package com.github.conserveorm.objects;

import java.io.Serializable;

import com.github.conserveorm.annotations.TableName;

/**
 * 
 * Simple test object with only primitive types.
 * 
 * @author Erik Berglund
 */
@TableName("SimpleObject")
public class SimpleObject implements Serializable
{
	private static final long serialVersionUID = -1629718480872790700L;
	private String name;
	private double value;
	private int count;
	private Double scale;
	private Long age;
	public String getName()
	{
		return name;
	}
	public void setName(String name)
	{
		this.name = name;
	}
	public double getValue()
	{
		return value;
	}
	public void setValue(double value)
	{
		this.value = value;
	}
	public int getCount()
	{
		return count;
	}
	public void setCount(int count)
	{
		this.count = count;
	}
	public Double getScale()
	{
		return scale;
	}
	public void setScale(Double scale)
	{
		this.scale = scale;
	}
	public Long getAge()
	{
		return age;
	}
	public void setAge(Long age)
	{
		this.age = age;
	}
}
