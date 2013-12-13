package org.conserve.tools.metadata;

import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import org.conserve.adapter.AdapterBase;
import org.conserve.annotations.AsBlob;
import org.conserve.annotations.AsClob;
import org.conserve.connection.ConnectionWrapper;
import org.conserve.exceptions.SchemaPermissionException;
import org.conserve.tools.Defaults;
import org.conserve.tools.DelayedInsertionBuffer;
import org.conserve.tools.NameGenerator;
import org.conserve.tools.ObjectTools;
import org.conserve.tools.Tools;
import org.conserve.tools.protection.ProtectionEntry;
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
	

	/**
	 * Save this object to the database. All properties will be saved. After
	 * this operation, the getId method will return the id of the database row.
	 * 
	 * Preconditions: The table for saving this object exists.
	 * 
	 * @param cw
	 *            the connection wrapper to use for the database operations.
	 * @param subClassName
	 *            the name of the direct subclass, if any. May be null.
	 * @param subClassId
	 *            the id of the subclass table row, if any. May be null.
	 * @throws SQLException
	 */
	public void save(ConnectionWrapper cw, String subClassName, Long subClassId)
			throws SQLException
	{
		if (subClassName != null && subClassId != null)
		{
			// store a reference to the subclass table entry
			addValuePair(Defaults.REAL_CLASS_COL, subClassName);
			addValuePair(Defaults.REAL_ID_COL, subClassId);
		}
		String stmt = getRowInsertionStatement();
		PreparedStatement ps = cw.prepareStatement(stmt);
		fillRowInsertionStatement(ps, cw);
		Tools.logFine(ps);
		ps.execute();
		ps.close();
		if (!isArray())
		{
			// store the id of the inserted row
			id = adapter.getPersist().getLastId(cw, getTableName());
			// save the protection entries
			protectionStack.save(this.getTableName(), id, cw);
		}
		else
		{
			// add the entries of the array
			id = adapter.getPersist().getLastId(cw, Defaults.ARRAY_TABLENAME);
			protectionStack.save(Defaults.ARRAY_TABLENAME, id, cw);
			adapter.getPersist().getArrayEntryWriter()
					.addArrayEntries(cw, id, object, delayBuffer);
		}
		if(delayBuffer!=null && object!=null)
		{
			delayBuffer.setUndefinedIds(id,System.identityHashCode(object));
		}
	}

	/**
	 * Statement to create the table for this type of object.
	 * 
	 * @return an SQL string that can be used to create the table for this
	 *         object.
	 * @throws SQLException
	 */
	public String getTableCreationStatement(ConnectionWrapper cw)
			throws SQLException
	{
		StringBuilder statement = new StringBuilder("CREATE TABLE ");
		statement.append(getTableName());
		statement.append(" (");
		ArrayList<String> columnDescriptions = new ArrayList<String>();
		// add the identifier, set it to a primary key
		if (adapter.isSupportsIdentity())
		{
			columnDescriptions.add(Defaults.ID_COL + " "
					+ adapter.getIdentity() + " PRIMARY KEY");
		}
		else
		{
			columnDescriptions.add(Defaults.ID_COL + " "
					+ adapter.getLongTypeKeyword() + " PRIMARY KEY");

		}

		if (this.clazz.isArray())
		{
			columnDescriptions.add(Defaults.ARRAY_MEMBER_ID + " "
					+ adapter.getReferenceType(Defaults.ARRAY_TABLENAME));
			columnDescriptions.add(Defaults.ARRAY_POSITION + " int ");
			columnDescriptions.add(Defaults.COMPONENT_CLASS_COL +" "
					+ adapter.getVarCharIndexed());
			columnDescriptions.add(Defaults.VALUE_COL + " "
					+ adapter.getColumnType(clazz.getComponentType(), null));
		}
		else
		{
			columnDescriptions.add(Defaults.REAL_CLASS_COL + " "
					+ adapter.getVarCharIndexed());
			columnDescriptions.add(Defaults.REAL_ID_COL + " bigint");
			for (int x = 0; x < this.getPropertyCount(); x++)
			{
				String mName = this.getPropertyName(x) + " ";
				if (this.isReferenceType(x))
				{
					Class<?> returnType = returnTypes.get(x);
					if (returnType.isInterface())
					{
						returnType = Object.class;
					}
					mName += adapter.getReferenceType(returnType);
				}
				else
				{
					mName += adapter.getColumnType(getReturnType(x),
							getAccessor(x));
				}
				columnDescriptions.add(mName);
				//add info to type table
				adapter.getPersist().getTableManager().addTypeInfo(getTableName(),getPropertyName(x),getReturnType(x),cw);
			}
		}
		for (int x = 0; x < columnDescriptions.size(); x++)
		{
			statement.append(columnDescriptions.get(x));
			if (x < columnDescriptions.size() - 1)
			{
				statement.append(", ");
			}
		}
		statement.append(")");
		return statement.toString();
	}

	public void ensureContainedTablesExist(ConnectionWrapper cw)
			throws SQLException, SchemaPermissionException
	{

		// a list of contained classes that must be added
		ArrayList<Class<?>> containedClasses = new ArrayList<Class<?>>();
		if (this.clazz.isArray())
		{
			if (!ObjectTools.isDatabasePrimitive(clazz.getComponentType()))
			{
				// make sure the component type exists
				containedClasses.add(clazz.getComponentType());
			}
		}
		else
		{
			for (int x = 0; x < this.getPropertyCount(); x++)
			{
				if (this.isReferenceType(x))
				{
					Class<?> returnType = returnTypes.get(x);
					if (returnType.isInterface())
					{
						returnType = Object.class;
					}
					containedClasses.add(returnType);
				}
			}
		}
		for (Class<?> c : containedClasses)
		{
			adapter.getPersist().getTableManager().ensureTableExists(c, cw);
		}
	}

	/**
	 * Generate the SQL statement that creates this object.
	 * 
	 * @return an SQL insert statement that can be used to create a
	 *         PreparedStatement used to insert this object.
	 */
	public String getRowInsertionStatement()
	{
		StringBuilder statement = new StringBuilder("INSERT INTO ");
		if (this.clazz.isArray())
		{
			statement.append(Defaults.ARRAY_TABLENAME);
			statement.append(" (");
			statement.append(Defaults.COMPONENT_TABLE_COL);
			statement.append(",");
			statement.append(Defaults.COMPONENT_CLASS_COL);
			statement.append(")VALUES(?,?)");
		}
		else
		{
			statement.append(getTableName());
			int nonNullCount = this.getNonNullPropertyCount();
			statement.append(" (");
			if (nonNullCount == 0 && !adapter.allowsEmptyStatements())
			{
				if (this.getPropertyCount() > 0)
				{
					// insert a null value for the first property
					statement.append(this.getPropertyName(0));
					statement.append(")VALUES(NULL");
				}
			}
			else
			{
				int addedCount = 0;
				for (int x = 0; x < props.size(); x++)
				{
					if (values.get(x) != null)
					{
						statement.append(props.get(x));
						addedCount++;
						if (addedCount < nonNullCount)
						{
							statement.append(", ");
						}
					}
				}
				// add the placeholders for the values
				statement.append(")VALUES(");
				addedCount = 0;
				for (int x = 0; x < values.size(); x++)
				{
					if (values.get(x) != null)
					{
						statement.append("?");
						addedCount++;
						if (addedCount < nonNullCount)
						{
							statement.append(", ");
						}
					}
				}
			}
			statement.append(")");
		}
		return statement.toString();
	}

	/**
	 * Fill in the values in the PreparedStatement, making it ready for
	 * execution.
	 * 
	 * @param ps
	 * @throws SQLException
	 */
	public void fillRowInsertionStatement(PreparedStatement ps,
			ConnectionWrapper cw) throws SQLException
	{
		if (this.isArray())
		{
			ps.setString(1, NameGenerator.getTableName(
					clazz.getComponentType(), adapter));
			if (clazz.getComponentType().isArray())
			{
				ps.setString(1, Defaults.ARRAY_TABLENAME);
			}
			// the table also contains the actual name of the class
			ps.setString(2, ObjectTools.getSystemicName(clazz));

		}
		else
		{
			// enter the values for the properties
			int index = 0;
			for (int x = 0; x < values.size(); x++)
			{
				Class<?> c = returnTypes.get(x);
				Object value = values.get(x);
				if (value != null)
				{
					index++;
					// find the type that best describes the object to store
					if (ObjectTools.isDatabasePrimitive(c))
					{
						Tools.setParameter(ps, c, index, value);
					}
					else
					{
						Long id = adapter.getPersist().saveObjectUnprotected(
								cw, value, delayBuffer);
						if (id == null)
						{
							ps.setNull(index, java.sql.Types.BIGINT);
							// this is a circularly referenced object
							// mark it for later insertion
							delayBuffer.add(getTableName(), getPropertyName(x),
									value, getReturnType(x),System.identityHashCode(this.object));
						}
						else
						{
							// get the correct id for the representative class
							if (!c.equals(value.getClass()))
							{
								Class<?> tempClass = c;
								if (c.isInterface())
								{
									// since we can't cast to an interface, just
									// cast to java.lang.Object.
									tempClass = Object.class;
								}
								long nuId = adapter.getPersist().getCastId(
										tempClass, value.getClass(), id, cw);
								ps.setLong(index, nuId);
							}
							else
							{
								ps.setLong(index, id);
							}
							// arrays are given as references to the
							// ARRAY_TABLE_NAME row that represents them
							if (value.getClass().isArray())
							{
								protectionStack.addEntry(new ProtectionEntry(
										Defaults.ARRAY_TABLENAME,null, id, props.get(x)));
							}
							else
							{
								protectionStack.addEntry(new ProtectionEntry(
										value.getClass(), id, props.get(x),
										adapter));
							}
						}
					}
				}
			}
		}
	}
}
