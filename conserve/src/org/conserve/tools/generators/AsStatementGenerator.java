package org.conserve.tools.generators;

import java.util.ArrayList;
import java.util.List;
import org.conserve.adapter.AdapterBase;

/**
 * Generates statements on the form "SOMETABLE AS A, SOMEOTHERTABLE AS B,[...]"
 * 
 * @author Erik Berglund
 *
 */
public class AsStatementGenerator
{

	private AdapterBase adapter;

	/**
	 * @param adapter
	 */
	public AsStatementGenerator(AdapterBase adapter)
	{
		this.adapter = adapter;
	}


	/**
	 * @param joinTableIds 
	 * @param joinTables 
	 * @param relationDescriptors
	 * @param omitTables table names to omit from the AS-statement.
	 * @return
	 */
	public String generate(List<String> joinTables, List<String> joinTableIds, List<RelationDescriptor> relationDescriptors,String []omitTables)
	{
		StringBuilder sb = new StringBuilder();
		List<String>added = new ArrayList<String>();
		//add the tables we are omitting to the list of added tables
		for(String omit:omitTables)
		{
			int omitIndex = joinTables.indexOf(omit);
			if(omitIndex>=0)
			{
				String omitId = joinTableIds.get(omitIndex);
				if(!added.contains(omitId))
				{
					added.add(omitId);
				}
			}
		}
		for(int x = 0;x<joinTables.size();x++)
		{
			if(!added.contains(joinTableIds.get(x)))
			{
				added.add(joinTableIds.get(x));
				if(sb.length()>0)
				{
					sb.append(" ");
					sb.append(adapter.getJoinKeyword());
					sb.append(" ");
				}
				sb.append(joinTables.get(x));
				sb.append(" AS ");
				sb.append(joinTableIds.get(x));
				
			}
		}
		for(RelationDescriptor desc:relationDescriptors)
		{
			FieldDescriptor fd = desc.getFirst();
			if(!added.contains(fd.getShortName()))
			{
				added.add(fd.getShortName());
				if(sb.length()>0)
				{
					sb.append(" ");
					sb.append(adapter.getJoinKeyword());
					sb.append(" ");
				}
				sb.append(fd.getTableName());
				sb.append(" AS ");
				sb.append(fd.getShortName());
			}
		}
		return sb.toString();
	}
}
