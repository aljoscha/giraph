package org.apache.giraph.examples.closeness;

import org.apache.hadoop.io.Writable;

/**
 * Very simple interface to enable having one VertexInputFormat for both the bit
 * field and the Flajolet-Martin Sketch Vertex.
 * 
 * @author Aljoscha Krettek
 * 
 */
public interface ClosenessVertexStateWritable extends Writable {
  public OpenLongIntHashMapWritable getShortestPaths();
}
