package org.apache.giraph.examples.closeness;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

/**
 * A counter for counting unique vertex ids using a bit field.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class BitfieldCounterWritable implements Writable {
  public final static String NUM_BITS = "distinctcounter.numbits";
  private int numBits;
  private int[] bits;

  /**
   * Create a zero-length bit field. This is needed because Giraph requires a
   * no-argument constructor.
   */
  public BitfieldCounterWritable() {
    // Empty if no bit length specified
    // in readFields we create a new bit array
    // when we recreate serialized counters
    this.numBits = 0;
    this.bits = new int[0];
  }

  /**
   * Create a bit field that can hold the specified number of bits, initially
   * all bits are zero.
   */
  public BitfieldCounterWritable(int numBits) {
    this.numBits = numBits;
    int numInts = (int) Math.ceil(this.numBits / 32.0);
    bits = new int[numInts];
  }

  /**
   * Create a copy of the bit field by copying the internal integer array.
   */
  public BitfieldCounterWritable copy() {
    BitfieldCounterWritable result = new BitfieldCounterWritable();
    result.numBits = this.numBits;
    result.bits = Arrays.copyOf(this.bits, this.bits.length);
    return result;
  }

  /**
   * Set the bit. If the bit is already set nothing happens.
   */
  public void addNode(long n) {
    int intIndex = (int) (n / 32);
    int bitIndex = (int) (n % 32);
    bits[intIndex] |= (1 << bitIndex);
  }

  /**
   * Return the number of set bits.
   */
  public int getCount() {
    int count = 0;
    for (int i = 0; i < bits.length; ++i) {
      int mask = 1;
      for (int j = 0; j < 32; ++j) {
        count += (bits[i] & mask) >> j;
        mask <<= 1;
      }
    }
    return count;
  }

  /**
   * Merge this bit field with the other bit field. The number of bits must
   * match.
   */
  public void merge(BitfieldCounterWritable other) {
    Preconditions.checkArgument(other instanceof BitfieldCounterWritable,
        "Other is not a BitfieldCounterWritable.");
    BitfieldCounterWritable otherB = (BitfieldCounterWritable) other;
    Preconditions.checkState(this.numBits == otherB.numBits,
        "Number of bits does not match: " + numBits + " other: "
            + otherB.numBits);
    for (int i = 0; i < bits.length; ++i) {
      bits[i] |= otherB.bits[i];
    }
  }

  /**
   * Return the number of bits in this bit field.
   */
  public int getNumBits() {
    return numBits;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(numBits);
    for (int b : bits) {
      out.writeInt(b);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    numBits = in.readInt();
    int numInts = (int) Math.ceil(numBits / 32.0);
    bits = new int[numInts];
    for (int i = 0; i < numInts; ++i) {
      bits[i] = in.readInt();
    }
  }
}
