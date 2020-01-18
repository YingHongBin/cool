/**
 * 
 */
package com.nus.cool.loader.reader;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author david
 *
 */
public interface TupleReader extends Closeable {

	boolean hasNext();
	
	Object next() throws IOException;
	
}
