/**
 * 
 */
package com.nus.cool.core.io.writestore;

import java.io.IOException;
import com.nus.cool.core.cohort.schema.FieldType;
import com.nus.cool.core.io.Output;

/**
 * Base interface for field write-only store. Implementation of
 * this interface should store the data according to the following layout
 * 
 * ---------------------------------------------------------
 * | fieldType | key codec | zKeys | value codec | zValues |
 * ---------------------------------------------------------
 * 
 * where index (key) codec is the compression scheme for index (keys) and value codec
 * is the compression scheme for values
 * 
 * @author david, xiezl
 *
 */
public interface FieldWS extends Output {
	
	FieldType getFieldType();
	
	void put(String[] tuple) throws IOException;
	
}
