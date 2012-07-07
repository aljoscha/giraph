package org.apache.giraph.examples.closeness;

import java.io.IOException;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.mahout.math.list.IntArrayList;

/**
 * VertexOutputFormat that supports {@link ClosenessVertex}
 */
public class ClosenessVertexOutputFormat extends
    TextVertexOutputFormat<IntWritable, VertexStateWritable, NullWritable> {

  @Override
  public VertexWriter<IntWritable, VertexStateWritable, NullWritable> createVertexWriter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    RecordWriter<Text, Text> recordWriter = textOutputFormat
        .getRecordWriter(context);
    return new ClosenessVertexOutputFormat.ClosenessVertexWriter(recordWriter);
  }

  /**
   * VertexWriter that supports {@link ClosenessVertex}
   */
  public static class ClosenessVertexWriter extends
      TextVertexWriter<IntWritable, VertexStateWritable, NullWritable> {
    /**
     * Vertex writer with the internal line writer.
     * 
     * @param lineRecordWriter
     *          will actually be written to.
     */
    public ClosenessVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
      super(lineRecordWriter);
    }

    @Override
    public void writeVertex(
        BasicVertex<IntWritable, VertexStateWritable, NullWritable, ?> vertex)
        throws IOException, InterruptedException {
      StringBuilder result = new StringBuilder();
      result.append(vertex.getVertexId().get());
      result.append("\t");
      OpenIntIntHashMapWritable shortestPaths = vertex.getVertexValue()
          .getShortestPaths();
      int numVerticesReachable = 0;
      int sumLengths = 0;
      System.out.println("VID: " + vertex.getVertexId().get());
      for (int key : shortestPaths.keys().elements()) {
        if (key < 1) {
          continue;
        }
        System.out.println("KEY: " + key);
        System.out.println("VAL: " + shortestPaths.get(key));
        int newlyReachable = shortestPaths.get(key) - shortestPaths.get(key-1);
        sumLengths += key * newlyReachable;
        if (shortestPaths.get(key) > numVerticesReachable) {
          numVerticesReachable = shortestPaths.get(key);
        }
      }
      
      double closeness = 0.0;
      if (numVerticesReachable > 0) {
        closeness = (double)sumLengths / (double)numVerticesReachable;
      }
      
      result.append(closeness);
      result.append("\t");
      
      IntArrayList keys = shortestPaths.keys();
      keys.sort();
      for (int key : keys.elements()) {
        result.append(key);
        result.append(":");
        result.append(shortestPaths.get(key));
        result.append(" ");
      }
      
      getRecordWriter().write(new Text(result.toString()), null);
    }
  }
}