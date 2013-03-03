package org.conserve.tools.metadata;

import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Clob;

import org.conserve.adapter.AdapterBase;
import org.conserve.annotations.AsBlob;
import org.conserve.annotations.AsClob;
import org.conserve.tools.Defaults;
import org.conserve.tools.DelayedInsertionBuffer;
import org.conserve.tools.NameGenerator;
import org.conserve.tools.ObjectTools;
import org.conserve.tools.protection.ProtectionStack;

/**
 * An object representation loaded by direct examination of the actual, in-memory object graph.
 * @author Erik Berglund
 *
 */
public class ConcreteObjectRepresentation extends ObjectRepresentation
{

	public ConcreteObjectRepresentation(AdapterBase adapter, Class<?> c,
			DelayedInsertionBuffer delayBuffer)
	{
		this(adapter, c, null, delayBuffer);
	}

	public ConcreteObjectRepresentation(AdapterBase adapter, Class<?> c, Object o,
			DelayedInsertionBuffer delayBuffer)
	{
		this.protectionStack = new ProtectionStack(adapter.getPersist()
				.getProtectionManager());
		this.delayBuffer = delayBuffer;
		this.adapter = adapter;
		this.object = o;
		this.clazz = c;
		Method[] methods = c.getDeclaredMethods();
		if (c.isArray())
		{
			tableName = NameGenerator.getArrayMemberTableName(
					c.getComponentType(), adapter);
		}
		else 
		{
			tableName = NameGenerator.getTableName(c, adapter);
		}
		for (Method m : methods)
		{
			if (ObjectTools.isValidMethod(m))
			{
				String name = NameGenerator.getColumnName(m);
				while (!adapter.isValidColumnName(name))
				{
					// create a valid name by pre-pending a string
					name = "C_" + name;
				}
				try
				{
					props.add(name);
					Method mutator = ObjectTools.getMutator(c,
							getMutatorName(m), m.getReturnType());
					setters.add(mutator);
					getters.add(m);
					Object value = null;
					if (o != null)
					{
						boolean oldAccessValue = m.isAccessible();
						m.setAccessible(true);
						value = m.invoke(o);
						values.add(value);
						m.setAccessible(oldAccessValue);
					}
					else
					{
						values.add(null);
					}

					if (m.isAnnotationPresent(AsClob.class)
							&& m.getReturnType().equals(char[].class)
							&& adapter.isSupportsClob())
					{
						returnTypes.add(Clob.class);
					}
					else if (m.isAnnotationPresent(AsBlob.class)
							&& m.getReturnType().equals(byte[].class)
							&& adapter.isSupportsBlob())
					{
						returnTypes.add(Blob.class);
					}
					else
					{
						returnTypes.add(m.getReturnType());
						if (adapter.isSupportsClob()
								&& m.isAnnotationPresent(AsClob.class))
						{
							LOGGER.warning("AsClob annotation is present on property "
									+ name
									+ " of class "
									+ ObjectTools.getSystemicName(clazz)
									+ ", but it does not have char[] return type.");
						}
						if (adapter.isSupportsBlob()
								&& m.isAnnotationPresent(AsBlob.class))
						{
							LOGGER.warning("AsBlob annotation is present on property "
									+ name
									+ " of class "
									+ ObjectTools.getSystemicName(clazz)
									+ ", but it does not have byte[] return type.");
						}
					}
				}
				catch (Exception e)
				{
					//can't recover, use generic catch
					e.printStackTrace();
				}
			}
		}
		if (ObjectTools.isDatabasePrimitive(c))
		{
			props.add(Defaults.VALUE_COL);
			returnTypes.add(c);
			// no need to add setter, primitives are cast from the raw types and
			// are final
			values.add(o);
		}
	}
}
