package org.apache.giraph.examples.closeness;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

public class FMCounterWritable implements Writable {
  public final static String NUM_BUCKETS = "fmsketch.numbuckets";
  private final static double MAGIC_CONSTANT = 0.77351;
  private final static int MAX_LENGHT = 32;
  private int numBuckets;
  private int[] buckets;
  boolean initialized = false;

  public FMCounterWritable() {
    this.numBuckets = 0;
    this.buckets = new int[numBuckets];
    initialized = true;
  }

  public FMCounterWritable(int numBuckets) {
    this.numBuckets = numBuckets;
    buckets = new int[this.numBuckets];
    initialized = true;
  }

  public FMCounterWritable copy() {
    FMCounterWritable result = new FMCounterWritable();
    result.numBuckets = this.numBuckets;
    result.buckets = Arrays.copyOf(this.buckets, this.buckets.length);
    result.initialized = true;
    return result;
  }

  private int firstOneBit(int value) {
    int index = 0;
    while ((value & 1) == 0 && index < 32) {
      ++index;
      value >>= 1;
    }
    return index;
  }

  public void addNode(int n) {
    int hash = new Integer(n).hashCode();
    // probably bad
    if (hash < 0) {
      hash *= -1;
    }
    if (numBuckets <= 0) {
      throw new RuntimeException("WHAT UP: " + initialized);
    }
    int bucketIndex = hash % numBuckets;
    int bitIndex = firstOneBit(hash / numBuckets);
    buckets[bucketIndex] |= (1 << bitIndex);
  }

  public int getCount() {
    int S = 0;
    for (int i = 0; i < buckets.length; ++i) {
      int R = 0;
      int bucket = buckets[i];
      while ((bucket & 1) == 1 && R < MAX_LENGHT) {
        ++R;
        bucket >>= 1;
      }
      S += R;
    }
    int count = (int) ((numBuckets / MAGIC_CONSTANT) * Math.pow(2, S
        / numBuckets));
    return count;
  }

  public void merge(FMCounterWritable other) {
    Preconditions.checkArgument(other instanceof FMCounterWritable,
        "Other is not a FMSketchWritable.");
    FMCounterWritable otherB = (FMCounterWritable) other;
    Preconditions.checkState(this.numBuckets == otherB.numBuckets,
        "Number of buckets does not match.");
    for (int i = 0; i < buckets.length; ++i) {
      buckets[i] |= otherB.buckets[i];
    }
  }
  
  public int getNumBuckets() {
    return numBuckets;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(numBuckets);
    for (int b : buckets) {
      out.writeInt(b);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    numBuckets = in.readInt();
    buckets = new int[numBuckets];
    for (int i = 0; i < numBuckets; ++i) {
      buckets[i] = in.readInt();
    }
    initialized = true;
  }
}
