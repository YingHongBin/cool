/**
 * 
 */
package com.nus.cool.core.io;

import java.io.DataInputStream;

/**
 * Re-implement Hadoop's DataInputBuffer since Hadoop's version
 * is not stable.
 * 
 * @author david
 *
 */
public class DataInputBuffer extends DataInputStream {
  private static class Buffer extends FastInputStream {
    public Buffer() {
      super(new byte[] {});
    }

    public void reset(byte[] input, int start, int length) {
      this.buf = input;
      this.count = start+length;
      this.mark = start;
      this.pos = start;
    }

    public byte[] getData() { return buf; }
    public int getPosition() { return pos; }
    public int getLength() { return count; }
  }

  private Buffer buffer;
  
  /** Constructs a new empty buffer. */
  public DataInputBuffer() {
    this(new Buffer());
  }

  private DataInputBuffer(Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  /** Resets the data that the buffer reads. */
  public void reset(byte[] input, int length) {
    buffer.reset(input, 0, length);
  }

  /** Resets the data that the buffer reads. */
  public void reset(byte[] input, int start, int length) {
    buffer.reset(input, start, length);
  }
  
  /** Resets the data that the buffer reads */
  public void reset(DataOutputBuffer input) {
  	reset(input.getData(), input.size());
  }
  
  public byte[] getData() {
    return buffer.getData();
  }

  /** Returns the current position in the input. */
  public int getPosition() { return buffer.getPosition(); }

  /**
   * Returns the index one greater than the last valid character in the input
   * stream buffer.
   */
  public int getLength() { return buffer.getLength(); }

}
