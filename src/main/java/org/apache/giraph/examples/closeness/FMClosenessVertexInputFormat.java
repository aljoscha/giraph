package org.apache.giraph.examples.closeness;

import java.io.IOException;
import java.util.Map;

import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Maps;

/**
 * VertexInputFormat that supports {@link ClosenessVertex}
 */
public class FMClosenessVertexInputFormat
    extends
    TextVertexInputFormat<LongWritable, FMVertexStateWritable, NullWritable, FMSketchWritable> {

  @Override
  public ClosenessVertexReader createVertexReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new ClosenessVertexReader();
  }

  /**
   * VertexReader that supports {@link ClosenessVertex}. In this case, the edge
   * values are not used. The files should be in the following JSON format:
   * JSONArray(<vertex id>, JSONArray(<dest vertex id>))
   */
  public class ClosenessVertexReader
      extends
      TextVertexInputFormat<LongWritable, FMVertexStateWritable, NullWritable, FMSketchWritable>.TextVertexReader {

    @Override
    public Vertex<LongWritable, FMVertexStateWritable, NullWritable, FMSketchWritable> getCurrentVertex()
        throws IOException, InterruptedException {
      Vertex<LongWritable, FMVertexStateWritable, NullWritable, FMSketchWritable> vertex = BspUtils
          .<LongWritable, FMVertexStateWritable, NullWritable, FMSketchWritable> createVertex(getContext()
              .getConfiguration());

      Text line = getRecordReader().getCurrentValue();
      String lineStr = line.toString();
      String parts[] = lineStr.split("\\t");
      LongWritable vertexId = new LongWritable(Integer.parseInt(parts[0]));

      String targetParts[] = parts[1].split(",");
      Map<LongWritable, NullWritable> edges = Maps.newHashMap();
      for (String targetStr : targetParts) {
        LongWritable targetId = new LongWritable(Integer.parseInt(targetStr));
        edges.put(targetId, NullWritable.get());
      }
      FMVertexStateWritable vertexState = new FMVertexStateWritable(getContext()
          .getConfiguration());
      vertexState.getCounter().addNode((int)vertexId.get());
      vertex.initialize(vertexId, vertexState, edges, null);
      return vertex;
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }
  }
}