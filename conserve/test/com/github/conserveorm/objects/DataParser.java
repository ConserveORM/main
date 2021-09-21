package com.github.conserveorm.objects;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * @author Erik Berglund
 *
 */
@SuppressWarnings("unused")
public class DataParser
{
	private static String ISO_DATE = "yyyy-MM-dd'T'HH:mm:ss'Z'";
	private static SimpleDateFormat ISO_FORMAT = new SimpleDateFormat(ISO_DATE);
	static
	{
		ISO_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	// Direction wind is coming from, in degrees
	private Integer windDirection;
	// Strength of wind, in knots
	private Integer windStrength;
	// Wind strength maximum, in knots.
	private Integer windStrengthMax;
	// Crosswind component, in knots
	private Integer crossWindStrength;
	// Minimum Runway Visual Range, metres
	private Integer minRvr;
	// if true, indicates that the actual RVR is over minRvr
	private Boolean minRvrOver;
	// Minimum visibility, metres
	private Integer minVisibility;

	// The type of the lowest cloud, SCT, BKN, OVC, FEW, VV, or null
	private CloudType lowestCloudType;

	// The height above ground of the lowest clouds, in feet
	private Integer lowestCloudFeet;

	// A description of the cloud ceiling, BKN, OVC, VV, or null
	private CloudType cloudCeilingType;
	// Cloud ceiling, feet (this is the lowest which covers more than half the
	// sky (4 oktas).
	// This is the lowest BKN or OVC.
	private Integer cloudCeilingFeet;
	// QNH pressure reduced to sea level at ISA temp, in hPa.
	private Float QNH;
	// Temperature, in degrees Celsius.
	private Float temperature;
	// Dew point, in degrees Celsius.
	private Float dewpoint;
	// Relative humidity, percent.
	private Integer relativeHumidity;
	// Precipitation string representation.
	private String precipitation;

	// the valid time. This is either the time in the document, or the time it
	// was parsed, whichever is first.
	private long validTime;

	/**
	 * Set the string from the DataGrabber.
	 * 
	 * @param data
	 */
	public void setString(String data)
	{
		String[] parts = data.split("\\n");
		if (parts.length < 17)
		{
			return;
		}
		int offset = 0;
		if (parts.length > 17)
		{
			offset = 20;
		}
		try
		{
			String[] qnhParts = parts[15 + offset].split(" ");
			QNH = Float.parseFloat(qnhParts[1]);
			String[] tmpParts = parts[16 + offset].split(" ");
			temperature = Float.parseFloat(tmpParts[1]);
			dewpoint = Float.parseFloat(tmpParts[3]);
			String rhString = tmpParts[5].replaceFirst("METAR", "");
			relativeHumidity = Integer.parseInt(rhString);
			precipitation = parts[9 + offset].substring(3).trim();
			
			if (precipitation.length() == 0)
			{
				precipitation = null;
			}
			parseRVR(parts[7 + offset]);
			parseVisibility(parts[8 + offset]);
			parseWindDirectionAndStrength(parts[3 + offset]);
			parseCloudCover(parts[10 + offset], parts[11 + offset], parts[12 + offset]);
			parseWindComposant(parts[6 + offset]);
			validTime = System.currentTimeMillis();
			long officialTime = ISO_FORMAT.parse(parts[0]).getTime();
			if (officialTime < validTime)
			{
				validTime = officialTime;
			}
		}
		catch (ParseException e)
		{
			e.printStackTrace();
		}
	}

	private void parseWindComposant(String string)
	{
		String[] parts = string.split(" ");
		String part = parts[1].split("/")[1];
		part = part.replaceAll("L", "");
		part = part.replaceAll("R", "");
		this.crossWindStrength = Integer.parseInt(part);
	}

	private void parseCloudCover(String... data)
	{
		lowestCloudFeet = null;
		lowestCloudType = null;
		cloudCeilingFeet = null;
		cloudCeilingType = null;

		try
		{
			// Get the lowest cloud cover
			String firstData = data[0].trim();
			if (firstData.length() > 0)
			{
				String[] parts = firstData.split(" ");
				if (parts.length > 2)
				{
					lowestCloudType = CloudType.valueOf(parts[1]);
					lowestCloudFeet = Integer.parseInt(parts[2]);
				}
			}
			// Get the ceiling
			for (String d : data)
			{
				String[] parts = d.split(" ");
				if (parts.length > 2)
				{
					CloudType ct = CloudType.valueOf(parts[1]);
					if (ct == CloudType.BKN || ct == CloudType.OVC || ct == CloudType.VV)
					{
						cloudCeilingType = ct;
						int cloudLayerFeet = Integer.parseInt(parts[2]);
						if (cloudCeilingFeet == null || cloudCeilingFeet > cloudLayerFeet)
						{
							cloudCeilingFeet = cloudLayerFeet;
						}
						// Only find the lowest ceiling
						break;
					}
				}
			}
		}
		catch (IllegalArgumentException e)
		{
			// Do nothing, we've got bad data
		}
	}

	private void parseWindDirectionAndStrength(String data)
	{
		String[] parts = data.split(" ");

		// take the average direction, rounded to nearest 10 degrees
		int degree1 = Integer.parseInt(parts[1].split("/")[0]);
		int degree2 = Integer.parseInt(parts[3].split("/")[0]);
		windDirection = (int) (10 * Math.round((degree1 + degree2) / 20.0));

		// take the highest wind speed
		int speed1 = Integer.parseInt(parts[1].split("/")[1]);
		int speed2 = Integer.parseInt(parts[3].split("/")[1]);
		windStrength = Math.max(speed1, speed2);
	}

	private void parseVisibility(String data)
	{
		String[] parts = data.split(" ");
		try
		{
			minVisibility = null;
			Integer value1 = getValue(parts[1]);
			Integer value2 = getValue(parts[3]);
			// Convert to metres, if needed
			if (value1 != null && parts[2].equalsIgnoreCase("km"))
			{
				value1 *= 1000;
			}
			if (value2 != null && parts[4].equalsIgnoreCase("km"))
			{
				value2 *= 1000;
			}
			if(value1!=null && value2!=null)
			{
				minVisibility = Math.min(value1, value2);
			}
			else if(value1!=null)
			{
				minVisibility = value1;
			}
			else if(value2!=null)
			{
				minVisibility = value2;
			}
		}
		catch(Exception e)
		{
			// Do nothing
		}
	}

	private void parseRVR(String rvrString)
	{
		String[] parts = rvrString.split(" ");
		// find the lowest value
		Integer[] ints = new Integer[2];
		ints[0] = getValue(parts[1]);
		ints[1] = getValue(parts[2]);
		minRvr = null;
		for (int x = 0; x < ints.length; x++)
		{
			if ((minRvr == null) || (ints[x] != null && ints[x] < minRvr))
			{
				minRvr = ints[x];
				if (parts[x + 1].startsWith(">"))
				{
					minRvrOver = true;
				}
				else
				{
					minRvrOver = false;
				}
			}
		}
	}

	// Gets the numerical value of any Arabic numerals in the string
	private Integer getValue(String string)
	{
		String buffer = "";
		for (int i = 0; i < string.length(); i++)
		{
			if (Character.isDigit(string.charAt(i)))
			{
				buffer += string.charAt(i);
			}
			else if (buffer.length() > 0)
			{
				break;
			}
		}
		if (buffer.length() > 0)
		{
			return Integer.parseInt(buffer);
		}
		else
		{
			return null;
		}
	}

	public int getWindDirection()
	{
		return windDirection;
	}

	/**
	 * Wind velocity in knots.
	 */
	public int getWindStrength()
	{
		return windStrength;
	}

	@SuppressWarnings("unused")
	private void setWindStrengthMax(Integer windStrengthMax)
	{
		this.windStrengthMax = windStrengthMax;
	}

	public Integer getWindStrengthMax()
	{
		return windStrengthMax;
	}

	public int getCrossWindStrength()
	{
		return crossWindStrength;
	}

	/**
	 * The minimum Runway Visual Range in metres, null if not known.
	 */
	public Integer getMinRvr()
	{
		return minRvr;
	}

	/**
	 * Get the minimum reported visibility in metres.
	 */
	public Integer getMinVisibility()
	{
		return minVisibility;
	}

	/**
	 * Return the cloud ceiling, in feet, or null if there is none.
	 * 
	 * @return
	 */
	public Integer getCloudCeilingFeet()
	{
		return cloudCeilingFeet;
	}

	public Float getQNH()
	{
		return QNH;
	}

	public Float getTemperature()
	{
		return temperature;
	}

	public Float getDewpoint()
	{
		return dewpoint;
	}

	public Integer getRelativeHumidity()
	{
		return relativeHumidity;
	}

	public String getPrecipitation()
	{
		return precipitation;
	}

	public long getValidTime()
	{
		return validTime;
	}

	public Boolean isMinRvrOver()
	{
		return minRvrOver;
	}

	private void setWindDirection(int windDirection)
	{
		this.windDirection = windDirection;
	}

	private void setWindStrength(int windStrength)
	{
		this.windStrength = windStrength;
	}

	private void setCrossWindStrength(int crossWindStrength)
	{
		this.crossWindStrength = crossWindStrength;
	}

	private void setMinRvr(Integer minRvr)
	{
		this.minRvr = minRvr;
	}

	private void setMinRvrOver(Boolean minRvrOver)
	{
		this.minRvrOver = minRvrOver;
	}

	private void setMinVisibility(Integer minVisibility)
	{
		this.minVisibility = minVisibility;
	}

	private void setCloudCeilingFeet(Integer cloudCeilingFeet)
	{
		this.cloudCeilingFeet = cloudCeilingFeet;
	}

	private void setQNH(Float qNH)
	{
		QNH = qNH;
	}

	private void setTemperature(Float temperature)
	{
		this.temperature = temperature;
	}

	private void setDewpoint(Float dewpoint)
	{
		this.dewpoint = dewpoint;
	}

	private void setRelativeHumidity(Integer relativeHumidity)
	{
		this.relativeHumidity = relativeHumidity;
	}

	private void setPrecipitation(String precipitation)
	{
		this.precipitation = precipitation;
	}

	private void setValidTime(long validTime)
	{
		this.validTime = validTime;
	}

	public CloudType getLowestCloudType()
	{
		return lowestCloudType;
	}

	private void setLowestCloudType(CloudType lowestCloudType)
	{
		this.lowestCloudType = lowestCloudType;
	}

	public Integer getLowestCloudFeet()
	{
		return lowestCloudFeet;
	}

	private void setLowestCloudFeet(Integer lowestCloudFeet)
	{
		this.lowestCloudFeet = lowestCloudFeet;
	}

	public CloudType getCloudCeilingType()
	{
		return cloudCeilingType;
	}

	private void setCloudCeilingType(CloudType cloudCeilingType)
	{
		this.cloudCeilingType = cloudCeilingType;
	}

	/**
	 * Calculate how long ago in milliseconds the report was created or
	 * downloaded, whichever is greater.
	 */
	public long getAge()
	{
		return System.currentTimeMillis() - validTime;
	}
}
