/**
 * 
 */
package com.nus.cohana.core.io;

import java.nio.ByteBuffer;

/**
 * The base interface for all read-only data structures
 * 
 * @author david
 *
 */
public interface Input {
	
	void readFrom(ByteBuffer buf);
	
}
