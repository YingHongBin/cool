/**
 * 
 */
package com.nus.cool.core.io;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author david
 *
 */
public interface AeolusWritable {

	void readFrom(ByteBuffer buffer);
	
	void writeTo(DataOutput out) throws IOException;
	
}
