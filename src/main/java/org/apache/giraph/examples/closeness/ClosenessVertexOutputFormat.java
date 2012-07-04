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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

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
      JSONArray jsonVertex = new JSONArray();
      jsonVertex.put(vertex.getVertexId().get());
      JSONObject values = new JSONObject();
      OpenIntIntHashMapWritable shortestPaths = vertex.getVertexValue()
          .getShortestPaths();
      for (int key : shortestPaths.keys().elements()) {
        try {
          values.put("" + key, "" + shortestPaths.get(key));
        } catch (JSONException e) {
        }
      }
      jsonVertex.put(values);
      getRecordWriter().write(new Text(jsonVertex.toString()), null);
    }
  }
}