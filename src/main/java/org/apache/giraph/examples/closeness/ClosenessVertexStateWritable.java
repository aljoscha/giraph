package org.apache.giraph.examples.closeness;

import org.apache.hadoop.io.Writable;

public interface ClosenessVertexStateWritable extends Writable {
  public OpenLongIntHashMapWritable getShortestPaths();
}
