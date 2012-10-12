package org.apache.giraph.examples.closeness;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The Writable that is used as Vertex value by {@link FMClosenessVertex}. This
 * holds the FM-Sketch (counter) and a hash map of shortest paths. (One could
 * probably also use an expanding array instead of the map but the map is
 * miniscule compared the the FM-Sketch so it does not matter.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class FMVertexStateWritable implements ClosenessVertexStateWritable {
  private FMCounterWritable counter;
  private OpenLongIntHashMapWritable shortestPaths;

  /**
   * Create a new vertex state with a zero-bucket FM-Sketch, this is required by
   * Giraph.
   */
  public FMVertexStateWritable() {
    this.counter = new FMCounterWritable();
    this.shortestPaths = new OpenLongIntHashMapWritable();
  }

  /**
   * Create a new vertex state with a FM-Sketch of the specified size.
   */
  public FMVertexStateWritable(int numBuckets) {
    this.counter = new FMCounterWritable(numBuckets);
    this.shortestPaths = new OpenLongIntHashMapWritable();
  }

  /**
   * Return the FM-Sketch.
   */
  public FMCounterWritable getCounter() {
    return counter;
  }

  /**
   * Return the shortest paths hash map.
   */
  public OpenLongIntHashMapWritable getShortestPaths() {
    return shortestPaths;
  }

  /**
   * Return the number of buckets in the FM-Sketch.
   */
  public int getNumBuckets() {
    return counter.getNumBuckets();
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
