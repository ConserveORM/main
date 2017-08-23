package com.github.conserveorm.annotations;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/** 
 * Annotation used to indicate that this field is part of the unique identifier of an object.
 * 
 * When an object of a class with this annotation on any of its accessors is saved, Conserve will
 * first check if the corresponding object already exists.
 * If it exists, the object will be updated (including setting all null fields to NULL in the database).
 * If it does not exist, a new row will be inserted in the database.
 * 
 * Any number of fields can be part of the id, just apply an Id annotation to all relevant accessors.
 * 
 * This annotation should be applied to the ACCESSOR method.
 * 
 * @author Erik Berglund
 *
 */
@Retention(RUNTIME)
@Target(METHOD)
public @interface Id
{

}
