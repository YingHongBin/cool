package com.nus.cohana.core.io.storevector;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;

import com.google.common.primitives.Shorts;
import com.nus.cohana.core.nio.ShortBuffers;

import com.nus.cohana.core.io.InputVector;
import com.nus.cohana.core.lang.Integers;

/**
 * Store integers in two bytes
 * 
 * @author david, caiqc, xiezl
 */
public class ZInt16Store implements ZIntStore, InputVector {

	private ShortBuffer shortBuffer;

	private int count;
	
	private ZInt16Store(int count) {
		this.count = count;
	}

	public ZInt16Store(byte[] compressed, int offset, int unused) {
		ByteBuffer b = ByteBuffer.wrap(compressed, offset, 4).order(ByteOrder.nativeOrder());
		count = b.getInt();
		int length = count * Shorts.BYTES;
		this.shortBuffer = ByteBuffer.wrap(compressed, offset + 4, length)
				.order(ByteOrder.nativeOrder()).asShortBuffer();
	}

	public static ZIntStore load(ByteBuffer buffer, int n) {	
		ZIntStore store = new ZInt16Store(n);
		store.readFrom(buffer);
		return store;
	}	

	@Override
	public int get(int pos) {
		return (shortBuffer.get(pos) & 0xFFFF);
	}

	@Override
	public void put(int[] val, int offset, int length) {
		// shortBuffer.rewind();
		for (int i = 0; i < length; i++) {
			shortBuffer.put(i, (short) val[i + offset]);
		}
	}

	@Override
	public void put(int pos, int val) {
		shortBuffer.put(pos, (short) val);
	}

	@Override
	public boolean hasNext() {
		return shortBuffer.hasRemaining();
	}

	@Override
	public int next() {
		return (shortBuffer.get() & 0xFFFF);
	}

	@Override
	public void readFrom(ByteBuffer buffer) {
		int limit = buffer.limit();
		int newlimit = buffer.position() + count * Shorts.BYTES;
		buffer.limit(newlimit);
		this.shortBuffer = buffer.asShortBuffer();
		buffer.position(newlimit);
		buffer.limit(limit);
	}

	@Override
	public void writeTo(DataOutput out) throws IOException {
		boolean bLittle = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
		out.writeInt(Integers.toNativeByteOrder(this.count));
		for (int i = 0; i < this.count; i++) {
			short s = shortBuffer.get(i);
			out.writeShort(bLittle ? Short.reverseBytes(s) : s);
		}
	}

	@Override
	public int binarySearch(int key) {
		return ShortBuffers.binarySearchUnsigned(shortBuffer, 0, shortBuffer.limit(),
				(short) key);
	}

	@Override
	public int size() {
		return this.count;
	}

	@Override
	public int sizeInByte() {
		return (this.count << 1);
	}

	@Override
	public void rewind() {
		this.shortBuffer.rewind();
	}

	@Override
	public int find(int key) {
		if (key > Short.MAX_VALUE || key < 0)
			return -1;

		return ShortBuffers.binarySearchUnsigned(shortBuffer, 0,
				shortBuffer.limit(), (short) key);
	}

	@Override
	public int sizeInBytes() {
		return shortBuffer.limit();
	}

	@Override
	public void skipTo(int pos) {
		shortBuffer.position(pos);
	}


}
