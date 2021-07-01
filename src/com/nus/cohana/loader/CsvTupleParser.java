/**
 * 
 */
package com.nus.cohana.loader;

/**
 * @author david
 *
 */
public class CsvTupleParser implements TupleParser {

	@Override
	public String[] parse(Object tuple) {
		String record = (String) tuple;
		return record.split(",", -1);
	}

}
