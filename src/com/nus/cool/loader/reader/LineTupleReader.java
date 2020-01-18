/**
 * 
 */
package com.nus.cool.loader.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Read tuples line by line
 * 
 * @author xiezl, david
 *
 */
public class LineTupleReader implements TupleReader {
	
	private String line;
	
	private BufferedReader reader;
	
	public LineTupleReader(File input) throws IOException {
		this.reader = new BufferedReader(new FileReader(input));
		this.line = reader.readLine();
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public boolean hasNext() {
		return line != null;
	}

	@Override
	public Object next() throws IOException {
		String old = line;
		line = reader.readLine();
		return old;
	}

}
