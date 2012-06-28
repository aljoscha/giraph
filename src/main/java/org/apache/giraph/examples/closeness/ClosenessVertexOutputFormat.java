package org.apache.giraph.examples.closeness;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.list.LongArrayList;

/**
 * VertexOutputFormat that writes the closeness (farness) value determined from
 * the shortest paths stored in a ClosenessVertexStateWritable map.
 */
public class ClosenessVertexOutputFormat<V extends ClosenessVertexStateWritable>
    extends TextVertexOutputFormat<LongWritable, V, NullWritable> {

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new ClosenessVertexWriter();
  }

  public class ClosenessVertexWriter extends TextVertexWriter {

    /**
     * Compute the closeness (farness) value as detailed in the paper. Note that
     * the value computed here is actually the farness, which is the inverse of
     * the closeness.
     */
    @Override
    public void writeVertex(Vertex<LongWritable, V, NullWritable, ?> vertex)
        throws IOException, InterruptedException {
      StringBuilder result = new StringBuilder();
      result.append(vertex.getId().get());
      result.append("\t");
      OpenLongIntHashMapWritable shortestPaths = vertex.getValue()
          .getShortestPaths();
      int numVerticesReachable = 0;
      int sumLengths = 0;
      for (long key : shortestPaths.keys().elements()) {
        if (key < 1) {
          continue;
        }
        int newlyReachable = shortestPaths.get(key)
            - shortestPaths.get(key - 1);
        sumLengths += key * newlyReachable;
        if (shortestPaths.get(key) > numVerticesReachable) {
          numVerticesReachable = shortestPaths.get(key);
        }
      }

      double closeness = 0.0;
      if (numVerticesReachable > 0) {
        closeness = (double) sumLengths / (double) numVerticesReachable;
      }

      result.append(closeness);
      result.append("\t");

      LongArrayList keys = shortestPaths.keys();
      keys.sort();
      for (long key : keys.elements()) {
        result.append(key);
        result.append(":");
        result.append(shortestPaths.get(key));
        result.append(" ");
      }

      getRecordWriter().write(new Text(result.toString()), null);
    }
  }
}