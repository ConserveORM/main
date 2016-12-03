package org.conserve.objects.demo;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This is a test object to check that the examples in the tutorial are correct.
 * 
 * @author Erik Berglund
 *
 */
public class TextObject
{
    private String text;
    private Set<String> keyWords;
    public TextObject(){}
	public String getText()
	{
		return text;
	}
	public void setText(String text)
	{
		this.text = text;
	}
	public Set<String> getKeyWords()
	{
		return keyWords;
	}
	public void setKeyWords(Set<String> keyWords)
	{
		this.keyWords = keyWords;
	}
	public void setKeyWords(String[] keywords)
	{
		setKeyWords(new HashSet<String>(Arrays.asList(keywords)));
		
	}
    
}
