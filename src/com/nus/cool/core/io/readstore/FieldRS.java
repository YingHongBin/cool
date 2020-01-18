/**
 * 
 */
package com.nus.cool.core.io.readstore;

import com.nus.cool.core.cohort.schema.FieldType;
import com.nus.cool.core.io.Input;
import com.nus.cool.core.io.InputVector;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * @author david
 *
 */
public interface FieldRS extends Input {
	
	FieldType getFieldType();
	
	/**
	 * Return the hash index vector. If the field is
	 * indexed by range indexing, an IllegalStateException
	 * is thrown.
	 *  
	 * @return
	 */
	InputVector getKeyVector();
	
	/**
	 * Returns the value vector of this field.
	 * 
	 * @return
	 */
	InputVector getValueVector();
	
	/**
	 * Returns the minKey if the field is range indexed. 
	 * IllegalStateException is thrown if the field is hash indexed.
	 * 
	 * @return
	 */
	int minKey();
	
	/**
	 * Returns the maxKey if the field is range indexed.
	 * IllegalStateException is thrown if the field is hash indexed.
	 * 
	 * @return
	 */
	int maxKey();
	
	/**
	 * Returns true if the field is a range indexed field
	 * @return
	 */
	boolean isRangeField();
	
	/**
	 * Returns true if the field is a hash(i.e., set) indexed field
	 * @return
	 */
	boolean isSetField();

	boolean isPreCal();

	BitSet[] getBitSets();

	void readFromWithFieldType(ByteBuffer buf, FieldType fieldType);
}
