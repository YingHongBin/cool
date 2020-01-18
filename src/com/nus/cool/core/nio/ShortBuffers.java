/**
 * 
 */
package com.nus.cool.core.nio;

import java.nio.ShortBuffer;

/**
 * @author david
 * @author qingchao
 *
 */
public class ShortBuffers {

	public static int binarySearch(ShortBuffer data, short key) {
		return binarySearch(data, data.position(), data.limit(), key);
	}
    
    public static int binarySearch(ShortBuffer data, int fromIndex, int toIndex,
			short key) {
		--toIndex;
		
		int ikey = key;
		while (fromIndex <= toIndex) {
			int mid = (fromIndex + toIndex) >> 1;
			int e = data.get(mid);
			if (ikey > e)
				fromIndex = mid + 1;
			else if (ikey < e)
				toIndex = mid - 1;
			else
				return mid; // key found
		}
		return ~fromIndex;
	}

	public static int binarySearchUnsigned(ShortBuffer data, short key) {
		return binarySearchUnsigned(data, data.position(), data.limit(), key);
	}

	public static int binarySearchUnsigned(ShortBuffer data, int fromIndex, int toIndex,
			short key) {
		--toIndex;
		
		// need to compare between the unsigned version of key and pivot
		// as both of them can be negative
		int ikey = key & 0xFFFF;
		while (fromIndex <= toIndex) {
			int mid = (fromIndex + toIndex) >> 1;
			int e = data.get(mid) & 0xFFFF;
			if (ikey > e)
				fromIndex = mid + 1;
			else if (ikey < e)
				toIndex = mid - 1;
			else
				return mid; // key found
		}
		return ~fromIndex;
	}
}
