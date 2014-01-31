package org.conserve.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/** 
 * Annotation used to indicate that this field should, when persisted in a database,
 * form part of the indices indicated by name.
 * 
 * A field can be part of any number of indices.
 * 
 * If a field is only included in one index, you can optionally use the {@link Indexed} annotation instead.
 * 
 * This annotation should be applied to the ACCESSOR method.
 * 
 * @author Erik Berglund
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface MultiIndexed
{
    /**
     * @return the name of the index this field will be part of
     */
    String []  values();
}