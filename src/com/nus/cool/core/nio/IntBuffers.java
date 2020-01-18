/**
 * 
 */
package com.nus.cool.core.nio;

import java.nio.IntBuffer;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author David
 *
 */
public class IntBuffers {

	public static int binarySearch(IntBuffer data, int key) {
		return binarySearch(data, data.position(), data.limit(), key);
	}

	public static int binarySearch(IntBuffer data, int fromIndex, int toIndex,
			int key) {
		checkNotNull(data);
		checkArgument(fromIndex < data.limit() && toIndex <= data.limit());
		
		--toIndex;
		while (fromIndex <= toIndex) {
			int mid = (fromIndex + toIndex) >> 1;
			int e = data.get(mid);
			if (key > e)
				fromIndex = mid + 1;
			else if (key < e)
				toIndex = mid - 1;
			else
				return mid; // key found
		}
		return ~fromIndex;
	}
	
}
