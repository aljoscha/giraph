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

package org.apache.giraph.io.hcatalog;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.mapreduce.HCatInputFormat;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;

/**
 * Abstract class that users should subclass to load data from a Hive or Pig
 * table. You can easily implement a {@link HCatalogVertexReader} by extending
 * either {@link SingleRowHCatalogVertexReader} or
 * {@link MultiRowHCatalogVertexReader} depending on how data for each vertex is
 * stored in the input table.
 * <p>
 * The desired database and table name to load from can be specified via
 * {@link HCatInputFormat#setInput(org.apache.hadoop.mapreduce.Job,
 * org.apache.hcatalog.mapreduce.InputJobInfo)}
 * as you setup your vertex input format with
 * {@link GiraphJob#setVertexInputFormatClass(Class)}.
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */

@SuppressWarnings("rawtypes")
public abstract class HCatalogVertexInputFormat<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        extends VertexInputFormat<I, V, E, M> {
  /**
  * H catalog input format.
  */
  private HCatInputFormat hCatInputFormat = new HCatInputFormat();

  @Override
  public final List<InputSplit> getSplits(
            final JobContext context, final int numWorkers)
    throws IOException, InterruptedException {
    return hCatInputFormat.getSplits(context);
  }

 /**
  * Abstract class that users should subclass
  * based on their specific vertex
  * input. HCatRecord can be parsed to get the
  * required data for implementing
  * getCurrentVertex(). If the vertex spans more
  * than one HCatRecord,
  * nextVertex() should be overwritten to handle that logic as well.
  */
  protected abstract class HCatalogVertexReader implements
        VertexReader<I, V, E, M> {

    /** Internal HCatRecordReader. */
    private RecordReader<WritableComparable,
                HCatRecord> hCatRecordReader;

    /** Context passed to initialize. */
    private TaskAttemptContext context;

    /**
     * Initialize with the HCatRecordReader.
     *
     * @param recordReader internal reader
     */
    private void initialize(
       final RecordReader<
                   WritableComparable, HCatRecord>
                   recordReader) {
      this.hCatRecordReader = recordReader;
    }

    @Override
    public final void initialize(
           final InputSplit inputSplit,
           final TaskAttemptContext ctxt)
      throws IOException, InterruptedException {
      hCatRecordReader.initialize(inputSplit, ctxt);
      this.context = ctxt;
    }

    @Override
    public boolean nextVertex()
      throws IOException, InterruptedException {
       // Users can override this if desired,
       // and a vertex is bigger than
       // a single row.
      return hCatRecordReader.nextKeyValue();
    }

    @Override
    public final void close() throws IOException {
      hCatRecordReader.close();
    }

    @Override
    public final float getProgress()
      throws IOException, InterruptedException {
      return hCatRecordReader.getProgress();
    }

    /**
    * Get the record reader.
    * @return Record reader to be used for reading.
    */
    protected final RecordReader<WritableComparable, HCatRecord>
    getRecordReader() {
      return hCatRecordReader;
    }

   /**
    * Get the context.
    *
    *
    *
    * @return Context passed to initialize.
    */
    protected final TaskAttemptContext getContext() {
      return context;
    }
  }

  /**
  * create vertex writer instance.
  * @return HCatalogVertexReader
  */
  protected abstract HCatalogVertexReader createVertexReader();

  @Override
  public final VertexReader<I, V, E, M>
  createVertexReader(final InputSplit split,
    final TaskAttemptContext context)
    throws IOException {
    try {
      HCatalogVertexReader reader = createVertexReader();
      reader.initialize(hCatInputFormat.
              createRecordReader(split, context));
      return reader;
    } catch (InterruptedException e) {
      throw new IllegalStateException(
               "createVertexReader: " +
                      "Interrupted creating reader.", e);
    }
  }

 /**
  * HCatalogVertexReader for tables holding
  * complete vertex info within each
  * row.
  */
  protected abstract class SingleRowHCatalogVertexReader
        extends HCatalogVertexReader {

    /**
    * 1024 const.
    */
    private static final int BYTE_CONST = 1024;

    /**
    *  logger
    */
    private final Logger log =
            Logger.getLogger(SingleRowHCatalogVertexReader.class);
    /**
    * record count.
    */
    private int recordCount = 0;
    /**
    * modulus check counter.
    */
    private final int recordModLimit = 1000;


    /**
    * get vertex id.
    * @param record hcat record
    * @return I id
         */
    protected abstract I getVertexId(HCatRecord record);

    /**
    * get vertext value.
    * @param record hcat record
    * @return V value
    */
    protected abstract V getVertexValue(HCatRecord record);

    /**
    * get edges.
    * @param record hcat record
    * @return Map edges
    */
    protected abstract Map<I, E> getEdges(HCatRecord record);

    @Override
    public final Vertex<I, V, E, M> getCurrentVertex()
      throws IOException, InterruptedException {
      HCatRecord record = getRecordReader().getCurrentValue();
      Vertex<I, V, E, M> vertex =
                    BspUtils.createVertex(getContext()
                            .getConfiguration());
      vertex.initialize(getVertexId(record), getVertexValue(record),
                    getEdges(record), null);
      ++recordCount;
      if ((recordCount % recordModLimit) == 0) {
        log.info("read " + recordCount + " records");
                // memory usage
        Runtime runtime = Runtime.getRuntime();
        double gb = BYTE_CONST *
                    BYTE_CONST *
                    BYTE_CONST;
        log.info("Memory: " + (runtime.totalMemory() / gb) +
                "GB total = " +
                ((runtime.totalMemory() - runtime.freeMemory()) / gb) +
                "GB used + " + (runtime.freeMemory() / gb) +
                "GB free, " + (runtime.maxMemory() / gb) + "GB max");
      }
      return vertex;
    }
  }
  /**
  * HCatalogVertexReader for tables
  * holding vertex info across multiple rows
  * sorted by vertex id column,
  * so that they appear consecutively to the
  * RecordReader.
  */
  protected abstract class MultiRowHCatalogVertexReader extends
            HCatalogVertexReader {
    /**
    * modulus check counter.
    */
    private static final int RECORD_MOD_LIMIT = 1000;

    /**
    *  logger
    */
    private final Logger log =
            Logger.getLogger(MultiRowHCatalogVertexReader.class);
    /**
    * current vertex id.
    */
    private I currentVertexId = null;
    /**
    * destination edge map.
    */
    private Map<I, E> destEdgeMap = Maps.newHashMap();
    /**
    * record for vertex.
    */
    private List<HCatRecord> recordsForVertex = Lists.newArrayList();
    /**
    * record count.
    */
    private int recordCount = 0;
    /**
    * vertex.
    *
    */
    private Vertex<I, V, E, M> vertex = null;
    /**
    * get vertex id from record.
    *
    * @param record hcat
    * @return I vertex id
    */
    protected abstract I getVertexId(HCatRecord record);

    /**
    * get vertex value from record.
    * @param records all vertex values
    * @return V iterable of record values
    */
    protected abstract V getVertexValue(
                Iterable<HCatRecord> records);

    /**
    * get target vertex id from record.
    *
    * @param record hcat
    * @return I vertex id of target.
    */
    protected abstract I getTargetVertexId(HCatRecord record);

    /**
    * get edge value from record.
    *
    * @param record hcat.
    * @return E edge value.
    */
    protected abstract E getEdgeValue(HCatRecord record);

    @Override
    public final Vertex<I, V, E, M>
    getCurrentVertex()
      throws IOException, InterruptedException {
      return vertex;
    }

    @Override
    public boolean nextVertex()
      throws IOException, InterruptedException {
      while (getRecordReader().nextKeyValue()) {
        HCatRecord record =
                getRecordReader().getCurrentValue();
        if (currentVertexId == null) {
          currentVertexId = getVertexId(record);
        }
        if (currentVertexId.equals(getVertexId(record))) {
          destEdgeMap.put(
            getTargetVertexId(record),
            getEdgeValue(record));
          recordsForVertex.add(record);
        } else {
          createCurrentVertex();
          if ((recordCount % RECORD_MOD_LIMIT) == 0) {
            log.info("read " + recordCount);
          }
          currentVertexId = getVertexId(record);
          recordsForVertex.add(record);
          return true;
        }
      }

      if (destEdgeMap.isEmpty()) {
        return false;
      } else {
        createCurrentVertex();
        return true;
      }
    }

    /**
    * create current vertex.
    */
    private void createCurrentVertex() {
      vertex = BspUtils.
              createVertex(getContext().getConfiguration());
      vertex.initialize(currentVertexId,
              getVertexValue(
                      recordsForVertex), destEdgeMap, null);
      destEdgeMap.clear();
      recordsForVertex.clear();
      ++recordCount;
    }
  }
}
