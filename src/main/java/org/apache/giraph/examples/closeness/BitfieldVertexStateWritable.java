package org.apache.giraph.examples.closeness;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The Writable that is used as Vertex value by {@link BitfieldClosenessVertex}.
 * This holds the bit field (counter) and a hash map of shortest paths. (One
 * could probably also use an expanding array instead of the map but the map is
 * miniscule compared the the bit field so it does not matter.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class BitfieldVertexStateWritable implements
    ClosenessVertexStateWritable {
  private BitfieldCounterWritable counter;
  private OpenLongIntHashMapWritable shortestPaths;

  /**
   * Create a new vertex state with a zero-length bit field, this is required by
   * Giraph.
   */
  public BitfieldVertexStateWritable() {
    this.counter = new BitfieldCounterWritable();
    this.shortestPaths = new OpenLongIntHashMapWritable();
  }

  /**
   * Create a new vertex state with a bit field of the specified size.
   */
  public BitfieldVertexStateWritable(int numBits) {
    counter = new BitfieldCounterWritable(numBits);
    shortestPaths = new OpenLongIntHashMapWritable();
  }

  /**
   * Return the bit field.
   */
  public BitfieldCounterWritable getCounter() {
    return counter;
  }

  /**
   * Return the shortest paths hash map.
   */
  public OpenLongIntHashMapWritable getShortestPaths() {
    return shortestPaths;
  }

  /**
   * Return the number of bits in the bit field counter.
   */
  public int getNumBits() {
    return counter.getNumBits();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    counter.write(out);
    shortestPaths.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    counter.readFields(in);
    shortestPaths.readFields(in);
  }
}
