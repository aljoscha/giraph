package org.apache.giraph.examples.closeness;

import java.io.IOException;
import java.util.Map;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Maps;

/**
 * VertexInputFormat that supports {@link ClosenessVertex}
 */
public class ClosenessVertexInputFormat
    extends
    TextVertexInputFormat<IntWritable, VertexStateWritable, NullWritable, BitfieldCounterWritable> {

  @Override
  public VertexReader<IntWritable, VertexStateWritable, NullWritable, BitfieldCounterWritable> createVertexReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new ClosenessVertexInputFormat.ClosenessVertexReader(
        textInputFormat.createRecordReader(split, context));
  }

  /**
   * VertexReader that supports {@link ClosenessVertex}. In this case, the edge
   * values are not used. The files should be in the following JSON format:
   * JSONArray(<vertex id>, JSONArray(<dest vertex id>))
   */
  public static class ClosenessVertexReader
      extends
      TextVertexReader<IntWritable, VertexStateWritable, NullWritable, BitfieldCounterWritable> {

    /**
     * Constructor with the line record reader.
     * 
     * @param lineRecordReader
     *          Will read from this line.
     */
    public ClosenessVertexReader(
        RecordReader<LongWritable, Text> lineRecordReader) {
      super(lineRecordReader);
    }

    @Override
    public BasicVertex<IntWritable, VertexStateWritable, NullWritable, BitfieldCounterWritable> getCurrentVertex()
        throws IOException, InterruptedException {
      BasicVertex<IntWritable, VertexStateWritable, NullWritable, BitfieldCounterWritable> vertex = BspUtils
          .<IntWritable, VertexStateWritable, NullWritable, BitfieldCounterWritable> createVertex(getContext()
              .getConfiguration());

      Text line = getRecordReader().getCurrentValue();
      String lineStr = line.toString();
      String parts[] = lineStr.split("\\t");
      IntWritable vertexId = new IntWritable(Integer.parseInt(parts[0]));

      String targetParts[] = parts[1].split(",");
      Map<IntWritable, NullWritable> edges = Maps.newHashMap();
      for (String targetStr : targetParts) {
        IntWritable targetId = new IntWritable(Integer.parseInt(targetStr));
        edges.put(targetId, NullWritable.get());
      }
      VertexStateWritable vertexState = new VertexStateWritable(getContext()
          .getConfiguration());
      vertexState.getCounter().addNode(vertexId.get());
      vertex.initialize(vertexId, vertexState, edges, null);
      return vertex;
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {

      return getRecordReader().nextKeyValue();
    }
  }
}