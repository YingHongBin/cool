package com.nus.cool.core.io.storevector;

import com.nus.cool.core.io.AeolusWritable;

/**
 * Backing stores for various chunk column dicts and ids.
 * 
 * Allowed access method: sequential & random
 * 
 * IMPORTANT: only next() function advances the position; all other put/get
 * functions do not change it.
 * 
 * @author qingchao
 * @author david
 */
public interface ZIntStore extends AeolusWritable {

	boolean hasNext();

	int next();

	/**
	 * Return the integer with the given index
	 * 
	 * @param index
	 * @return
	 */
	int get(int index);

	@Deprecated
	void put(int[] val, int offset, int length);

	@Deprecated
	void put(int index, int val);

	int binarySearch(int key);

	int size();

	int sizeInByte();

	void rewind();

}
