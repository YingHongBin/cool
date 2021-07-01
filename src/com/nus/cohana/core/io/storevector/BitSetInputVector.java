/**
 * 
 */
package com.nus.cohana.core.io.storevector;

import java.nio.ByteBuffer;
import java.util.BitSet;

import com.google.common.primitives.Longs;

import com.nus.cohana.core.io.InputVector;

/**
 * @author david, xiezl
 *
 */
public class BitSetInputVector implements InputVector {
	
	private long[] words;

	private int[] lookupTable;

	private int[] globalIDsArray = new int[0];

	private int numOfIDs;

	private final void fillInGlobalIDs() {
		BitSet bs = BitSet.valueOf(this.words);
		globalIDsArray = new int[numOfIDs];
		for (int i = bs.nextSetBit(0), j = 0; i >= 0; i = bs.nextSetBit(i + 1)) {
			globalIDsArray[j++] = i;
		}
	}
	
	private final int wordIndex(int i) {
		return i >>> 6;
	}

	private final int remainder(int i) {
		return i & (64 - 1);
	}
	
	@Override
	public void readFrom(ByteBuffer buffer) {
		int len = buffer.get() & 0xFF;
		this.words = new long[len];
		this.lookupTable = new int[len];

		this.lookupTable[0] = 0;
		this.words[0] = buffer.getLong();

		for (int i = 1; i < len; i++) {
			words[i] = buffer.getLong();
			lookupTable[i] =  Long.bitCount(words[i - 1]) + lookupTable[i - 1];
		}

		numOfIDs = lookupTable[len - 1] + Long.bitCount(words[len - 1]);
	}

	@Override
	public int find(int key) {
		int i = wordIndex(key);
		int j = remainder(key);
		long bits = words[i] << (63 - j);
		return (bits < 0) ? (Long.bitCount(bits) + lookupTable[i] - 1) :  -1;
	}

	@Override
	public int get(int index) {
		if (globalIDsArray.length == 0) {
			fillInGlobalIDs();
		}
		return globalIDsArray[index];
	}

	@Override
	public boolean hasNext() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int next() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int size() {
		int last = lookupTable.length - 1;
		return (lookupTable[last] & 0xFF) + Long.bitCount(words[last]);
	}

	@Override
	public int sizeInBytes() {
		return words.length * Longs.BYTES;
	}

	@Override
	public void skipTo(int pos) {
		throw new UnsupportedOperationException();
	}

}
