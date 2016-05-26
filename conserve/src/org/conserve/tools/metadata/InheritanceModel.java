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
package org.conserve.tools.metadata;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.conserve.connection.ConnectionWrapper;
import org.conserve.tools.Defaults;
import org.conserve.tools.NameGenerator;
import org.conserve.tools.Tools;

/**
 * 
 * Represents the inheritance of a given class, as loaded from the database.
 * This is used for detecting changes in the intheritance of a class for purposes of schema updating.
 * 
 * @author Erik Berglund
 *
 */
public class InheritanceModel
{
	private Class<?> klass;
	private Class<?>superclass;
	private List<Class<?>>interfaces = new ArrayList<Class<?>>();
	private List<Class<?>>directSubclasses=new ArrayList<Class<?>>();

	public InheritanceModel(Class<?>klass,ConnectionWrapper cw) throws SQLException
	{
		this.klass=klass;
		try
		{
			loadFromDb(cw);
		}
		catch (ClassNotFoundException e)
		{
			throw new SQLException(e);
		}
	}

	/**
	 * Read the inheritance info from the database.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * 
	 */
	private void loadFromDb(ConnectionWrapper cw) throws SQLException, ClassNotFoundException
	{
		// load superclass and interfaces
		PreparedStatement stmnt = cw.prepareStatement("SELECT SUPERCLASS FROM " + Defaults.IS_A_TABLENAME+" WHERE SUBCLASS=?");
		stmnt.setString(1, NameGenerator.getSystemicName(klass));
		Tools.logFine(stmnt);
		ResultSet rs = stmnt.executeQuery();
		while(rs.next())
		{
			String name = rs.getString(1);
			Class<?> c = ClassLoader.getSystemClassLoader().loadClass(name);
			if(c.isInterface() && !klass.isInterface())
			{
				interfaces.add(c);
			}
			else
			{
				superclass=c;
			}
		}
		stmnt.close();
		
		//load direct subclasses
		stmnt = cw.prepareStatement("SELECT SUBCLASS FROM "+ Defaults.IS_A_TABLENAME+" WHERE SUPERCLASS =?");
		stmnt.setString(1, NameGenerator.getSystemicName(klass));
		Tools.logFine(stmnt);
		rs = stmnt.executeQuery();
		while(rs.next())
		{
			String name = rs.getString(1);
			Class<?> c = ClassLoader.getSystemClassLoader().loadClass(name);
			directSubclasses.add(c);
		}
	}

	/**
	 * Return the superclass of the modelled class.
	 * 
	 * @return the superclass
	 */
	public Class<?> getSuperclass()
	{
		return superclass;
	}

	/**
	 * Return all interfaces of the modelled class.
	 * 
	 * @return the interfaces
	 */
	public List<Class<?>> getInterfaces()
	{
		return interfaces;
	}
	
	public List<Class<?>>getDirectSubclasses()
	{
		return directSubclasses;
	}
	
	
}
