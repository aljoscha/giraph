package org.apache.giraph.examples.closeness;

import java.io.IOException;
import java.util.Map;

import org.apache.giraph.io.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Maps;

/**
 * VertexInputFormat that supports {@link BitfieldClosenessVertex} and
 * {@FMClosenessVertex}.
 */
public class ClosenessVertexInputFormat<V extends Writable, M extends Writable>
    extends TextVertexInputFormat<LongWritable, V, NullWritable, M> {

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new ClosenessVertexReader();
  }

  /**
   * VertexReader that supports {@link BitfieldClosenessVertex}. In this case,
   * the edge values are not used. The files should be in the following format:
   * <vertex id>\t<dest vertex ids> where <dest vertex ids> is a comma separated
   * list of numbers
   */
  public class ClosenessVertexReader extends
      TextVertexReaderFromEachLineProcessed<String[]> {

    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      return line.toString().split("\\t");
    }

    @Override
    protected LongWritable getId(String[] tokens) throws IOException {
      return new LongWritable(Integer.parseInt(tokens[0]));
    }

    @Override
    protected V getValue(String[] tokens) throws IOException {
      return null;
    }

    @Override
    protected Map<LongWritable, NullWritable> getEdges(String[] tokens)
        throws IOException {
      String targetParts[] = tokens[1].split(",");
      Map<LongWritable, NullWritable> edges = Maps.newHashMap();
      for (String targetStr : targetParts) {
        LongWritable targetId = new LongWritable(Integer.parseInt(targetStr));
        edges.put(targetId, NullWritable.get());
      }
      return edges;
    }
  }
}