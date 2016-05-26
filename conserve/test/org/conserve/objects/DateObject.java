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
package org.conserve.objects;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * An object that encapsulates a java.sql.Date object for test purposes.
 * 
 * @author Erik Berglund
 *
 */
public class DateObject
{
	private Date date;
	private Time time;
	/**
	 * @return the time
	 */
	public Time getTime()
	{
		return time;
	}
	/**
	 * @param time the time to set
	 */
	public void setTime(Time time)
	{
		this.time = time;
	}
	/**
	 * @return the timeStamp
	 */
	public Timestamp getTimeStamp()
	{
		return timeStamp;
	}
	/**
	 * @param timeStamp the timeStamp to set
	 */
	public void setTimeStamp(Timestamp timeStamp)
	{
		this.timeStamp = timeStamp;
	}
	private Timestamp timeStamp;
	
	public void setDate(Date date)
	{
		this.date = date;
	}
	public Date getDate()
	{
		return date;
	}
}
