/**
 * 
 */
package com.nus.cool.core.io.storevector;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import com.nus.cool.core.io.InputVector;

/**
 * Note: this class is only used for reading LZ4 compressed string values. It
 * can NOT be used for reading compressed integers.
 * 
 * @author david
 *
 */
public class LZ4InputVector implements InputVector {

	private byte[] data;

	private int[] offsets;

	private int zLen;

	private int rawLen;

	private ByteBuffer buffer;

	private LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance()
			.fastDecompressor();

	@Override
	public void readFrom(ByteBuffer buf) {
		this.zLen = buf.getInt();
		this.rawLen = buf.getInt();
		int oldLimit = buf.limit();
		int newLimit = buf.position() + zLen;
		buf.limit(newLimit);
		this.buffer = buf.slice().order(buf.order());
		buf.position(newLimit);
		buf.limit(oldLimit);
	}

	@Override
	public int find(int key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int get(int index) {
		throw new UnsupportedOperationException();
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
		throw new UnsupportedOperationException();
	}

	@Override
	public int sizeInBytes() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void skipTo(int pos) {
		throw new UnsupportedOperationException();
	}

	public String getString(int index, Charset charset) {
		// Lazy decompression
		if (buffer.hasRemaining()) {
			byte[] compressed = new byte[zLen];
			byte[] raw = new byte[rawLen];
			buffer.get(compressed);
			decompressor.decompress(compressed, raw, rawLen);
			ByteBuffer buf = ByteBuffer.wrap(raw);
			int values = buf.getInt();
			this.offsets = new int[values];
			for (int i = 0; i < values; i++)
				this.offsets[i] = buf.getInt();
			this.data = new byte[rawLen - 4 - values * 4];
			buf.get(this.data);
		}
		checkArgument(index < offsets.length && index >= 0);
		int last = offsets.length - 1;
		int off = offsets[index];
		int end = index == last ? data.length : offsets[index + 1];
		int len = end - off;
		byte[] tmp = new byte[len];
		System.arraycopy(data, off, tmp, 0, len);
		return new String(tmp, charset);
	}
}
