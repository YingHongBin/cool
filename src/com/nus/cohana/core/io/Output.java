/**
 * 
 */
package com.nus.cohana.core.io;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Base interface for all write-only data structures
 * 
 * @author david
 *
 */
public interface Output {

  int writeTo(DataOutput out) throws IOException;

}
