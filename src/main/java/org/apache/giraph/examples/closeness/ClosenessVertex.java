/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples.closeness;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.giraph.lib.TextVertexInputFormat.TextVertexReader;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.giraph.lib.TextVertexOutputFormat.TextVertexWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class ClosenessVertex
    extends
    EdgeListVertex<LongWritable, VertexStateWritable, IntWritable, BitfieldCounterWritable>
    implements Tool {

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ClosenessVertex.class);

  /** Configuration */
  private Configuration conf;

  @Override
  public void compute(Iterator<BitfieldCounterWritable> msgIterator) {
    int seenCountBefore = getVertexValue().getCounter().getCount();

    while (msgIterator.hasNext()) {
      BitfieldCounterWritable inCounter = msgIterator.next();
      getVertexValue().getCounter().merge(inCounter);
    }

    int seenCountAfter = getVertexValue().getCounter().getCount();

    if ((seenCountBefore != seenCountAfter) || (getSuperstep() == 0)) {
      sendMsgToAllEdges(getVertexValue().getCounter().copy());
    }

    // subtract 1 because our own bit is counted as well
    getVertexValue().getShortestPaths().put(getSuperstep(),
        getVertexValue().getCounter().getCount() - 1);

    voteToHalt();
  }

  /**
   * VertexInputFormat that supports {@link ClosenessVertex}
   */
  public static class ClosenessVertexInputFormat
      extends
      TextVertexInputFormat<LongWritable, VertexStateWritable, IntWritable, BitfieldCounterWritable> {
    @Override
    public VertexReader<LongWritable, VertexStateWritable, IntWritable, BitfieldCounterWritable> createVertexReader(
        InputSplit split, TaskAttemptContext context) throws IOException {
      return new ClosenessVertexReader(textInputFormat.createRecordReader(
          split, context));
    }
  }

  /**
   * VertexReader that supports {@link ClosenessVertex}. In this case, the edge
   * values are not used. The files should be in the following JSON format:
   * JSONArray(<vertex id>, JSONArray(<dest vertex id>))
   */
  public static class ClosenessVertexReader
      extends
      TextVertexReader<LongWritable, VertexStateWritable, IntWritable, BitfieldCounterWritable> {

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
    public BasicVertex<LongWritable, VertexStateWritable, IntWritable, BitfieldCounterWritable> getCurrentVertex()
        throws IOException, InterruptedException {
      BasicVertex<LongWritable, VertexStateWritable, IntWritable, BitfieldCounterWritable> vertex = BspUtils
          .<LongWritable, VertexStateWritable, IntWritable, BitfieldCounterWritable> createVertex(getContext()
              .getConfiguration());

      Text line = getRecordReader().getCurrentValue();
      try {
        JSONArray jsonVertex = new JSONArray(line.toString());
        LongWritable vertexId = new LongWritable(jsonVertex.getLong(0));
        Map<LongWritable, IntWritable> edges = Maps.newHashMap();
        JSONArray jsonEdgeArray = jsonVertex.getJSONArray(1);
        for (int i = 0; i < jsonEdgeArray.length(); ++i) {
          LongWritable targetId = new LongWritable(jsonEdgeArray.getLong(i));
          edges.put(targetId, new IntWritable(1));
        }
        VertexStateWritable vertexState = new VertexStateWritable(getContext()
            .getConfiguration());
        vertexState.getCounter().addNode(vertexId.get());
        vertex.initialize(vertexId, vertexState, edges, null);
      } catch (JSONException e) {
        throw new IllegalArgumentException(
            "next: Couldn't get vertex from line " + line, e);
      }
      return vertex;
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {

      return getRecordReader().nextKeyValue();
    }
  }

  /**
   * VertexOutputFormat that supports {@link ClosenessVertex}
   */
  public static class ClosenessVertexOutputFormat extends
      TextVertexOutputFormat<LongWritable, VertexStateWritable, IntWritable> {
    @Override
    public VertexWriter<LongWritable, VertexStateWritable, IntWritable> createVertexWriter(
        TaskAttemptContext context) throws IOException, InterruptedException {
      RecordWriter<Text, Text> recordWriter = textOutputFormat
          .getRecordWriter(context);
      return new ClosenessVertexWriter(recordWriter);
    }
  }

  /**
   * VertexWriter that supports {@link ClosenessVertex}
   */
  public static class ClosenessVertexWriter extends
      TextVertexWriter<LongWritable, VertexStateWritable, IntWritable> {
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
        BasicVertex<LongWritable, VertexStateWritable, IntWritable, ?> vertex)
        throws IOException, InterruptedException {
      JSONArray jsonVertex = new JSONArray();
      jsonVertex.put(vertex.getVertexId().get());
      JSONObject values = new JSONObject();
      OpenLongLongHashMapWritable shortestPaths = vertex.getVertexValue()
          .getShortestPaths();
      for (long key : shortestPaths.keys().elements()) {
        try {
          values.put("" + key, "" + shortestPaths.get(key));
        } catch (JSONException e) {
        }
      }
      jsonVertex.put(values);
      getRecordWriter().write(new Text(jsonVertex.toString()), null);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * hadoop jar target/giraph-0.2-SNAPSHOT-jar-with-dependencies.jar
   * org.apache.giraph.examples.closeness.ClosenessVertex closenessInputGraph
   * closenessOutputGraph 32 3
   */
  @Override
  public int run(String[] argArray) throws Exception {
    Preconditions.checkArgument(argArray.length == 4,
        "run: Must have 4 arguments <input path> <output path> "
            + "<num bits> <# of workers>");

    GiraphJob job = new GiraphJob(getConf(), getClass().getName());
    job.setVertexClass(getClass());
    job.setVertexInputFormatClass(ClosenessVertexInputFormat.class);
    job.setVertexOutputFormatClass(ClosenessVertexOutputFormat.class);
    FileInputFormat.addInputPath(job.getInternalJob(), new Path(argArray[0]));
    FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(argArray[1]));
    job.getConfiguration().setInt(BitfieldCounterWritable.NUM_BITS,
        Integer.parseInt(argArray[2]));
    job.setWorkerConfiguration(Integer.parseInt(argArray[3]),
        Integer.parseInt(argArray[3]), 100.0f);

    return job.run(true) ? 0 : -1;
  }

  /**
   * Can be used for command line execution.
   * 
   * @param args
   *          Command line arguments.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new ClosenessVertex(), args));
  }
}
