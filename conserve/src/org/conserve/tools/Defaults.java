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
package org.conserve.tools;


/**
 * @author Erik Berglund
 *
 */
public class Defaults
{
	/**
	 * Default table names.
	 */
	public static final String IS_A_TABLENAME = "C__IS_A";
	public static final String HAS_A_TABLENAME = "C__HAS_A";
	public static final String ARRAY_TABLENAME = "C__ARRAY";
	public static final String ARRAY_MEMBER_TABLENAME = "C__ARRAY_MEMBER_";
	public static final String SCHEMA_VERSION_TABLENAME = "C__SCHEMA_VERSION";
	public static final String TYPE_TABLENAME = "C__TYPE_TABLE";
	public static final String TABLE_NAME_TABLENAME = "C__TABLE_NAME";
	public static final String INDEX_TABLENAME = "C__INDEX";
	public static final String CLASS_NAME_MAP_TABLE = "C__CLASS_NAME_MAP";
	public static final String TABLE_NAME_MAP_TABLE = "C__TABLE_NAME_MAP";

	/**
	 * Default columns.
	 */
	public static final String REAL_CLASS_COL = "C__REALCLASS";
	public static final String ID_COL = "C__ID";
	public static final String COLLECTION_PROPERTY_COL = "C__COLLECTION_CONTENTS";
	public static final String MAP_PROPERTY_COL = "C__MAP_CONTENTS";
	public static final String ARRAY_MEMBER_ID = "C__ARRAY_MEMBER_ID";
	public static final String ARRAY_POSITION = "C__POSITION";
	public static final String VALUE_COL = "C__VALUE";
	public static final String COMPONENT_TABLE_COL = "COMPONENT_TABLE";
	public static final String COMPONENT_CLASS_COL = "COMPONENT_TYPE";
	public static final String RELATION_NAME_COL = "RELATION_NAME";
	
	/**
	 * Placeholders.
	 */
	public static final String SUBQUERY_PLACEHOLDER = "<<SUBQUERY>>";
	public static final String TABLENAME_PLACEHOLDER = "<<TABLENAME>>";
	public static final String OLD_COLUMN_NAME_PLACEHOLDER = "<<OLD_COLUMN_NAME>>";
	public static final String NEW_COLUMN_NAME_PLACEHOLDER = "<<NEW_COLUMN_NAME>>";
	public static final String NEW_COLUMN_DESCRIPTION_PLACEHOLDER = "<<NEW_COLUMN_DESCRIPTION>>";

	
	/**
	 * Composite defaults.
	 */
	public static final String ARRAY_MEMBER_TABLE_NAME_ARRAY = ARRAY_MEMBER_TABLENAME+ARRAY_TABLENAME;
}
