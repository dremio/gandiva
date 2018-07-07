/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.gandiva.evaluator;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

interface DataAndVectorGenerator {
  public void writeData(ArrowBuf buffer);
  public ValueVector generateOutputVector(int numRowsInBatch);
}

class Int32DataAndVectorGenerator implements DataAndVectorGenerator {
  protected final BufferAllocator allocator;
  protected final Random rand;

  Int32DataAndVectorGenerator(BufferAllocator allocator) {
    this.allocator = allocator;
    this.rand = new Random();
  }

  @Override
  public void writeData(ArrowBuf buffer) {
    buffer.writeInt(rand.nextInt());
  }

  @Override
  public ValueVector generateOutputVector(int numRowsInBatch) {
    IntVector intVector = new IntVector(BaseNativeEvaluatorTest.EMPTY_SCHEMA_PATH, allocator);
    intVector.allocateNew(numRowsInBatch);
    return intVector;
  }
}

class BoundedInt32DataAndVectorGenerator extends Int32DataAndVectorGenerator {
  private final int upperBound;

  BoundedInt32DataAndVectorGenerator(BufferAllocator allocator, int upperBound) {
    super(allocator);
    this.upperBound = upperBound;
  }

  @Override
  public void writeData(ArrowBuf buffer) {
    buffer.writeInt(rand.nextInt(upperBound));
  }
}

class BaseNativeEvaluatorTest {
  protected final static int THOUSAND = 1000;
  protected final static int MILLION = THOUSAND * THOUSAND;

  protected final static String EMPTY_SCHEMA_PATH = "";

  protected BufferAllocator allocator;
  protected ArrowType boolType;
  protected ArrowType int8;
  protected ArrowType int32;
  protected ArrowType int64;
  protected ArrowType float64;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    boolType = new ArrowType.Bool();
    int8 = new ArrowType.Int(8, true);
    int32 = new ArrowType.Int(32, true);
    int64 = new ArrowType.Int(64, true);
    float64 = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  ArrowBuf buf(byte[] bytes) {
    ArrowBuf buffer = allocator.buffer(bytes.length);
    buffer.writeBytes(bytes);
    return buffer;
  }

  ArrowBuf arrowBufWithAllValid(int size) {
    int bufLen = (size + 7) / 8;
    ArrowBuf buffer = allocator.buffer(bufLen);
    for(int i = 0; i < bufLen; i++) {
      buffer.writeByte(255);
    }

    return buffer;
  }

  ArrowBuf intBuf(int[] ints) {
    ArrowBuf buffer = allocator.buffer(ints.length * 4);
    for (int i = 0; i < ints.length; i++) {
      buffer.writeInt(ints[i]);
    }
    return buffer;
  }

  ArrowBuf longBuf(long[] longs) {
    ArrowBuf buffer = allocator.buffer(longs.length * 8);
    for (int i = 0; i < longs.length; i++) {
      buffer.writeLong(longs[i]);
    }
    return buffer;
  }

  ArrowBuf doubleBuf(double[] data) {
    ArrowBuf buffer = allocator.buffer(data.length * 8);
    for (int i = 0; i < data.length; i++) {
      buffer.writeDouble(data[i]);
    }

    return buffer;
  }

  ArrowBuf stringToMillis(String[] dates) {
    ArrowBuf buffer = allocator.buffer(dates.length * 8);
    for(int i = 0; i < dates.length; i++) {
      Instant instant = Instant.parse(dates[i]);
      buffer.writeLong(instant.getMillis());
    }

    return buffer;
  }

  void releaseRecordBatch(ArrowRecordBatch recordBatch) {
    // There are 2 references to the buffers
    // One in the recordBatch - release that by calling close()
    // One in the allocator - release that explicitly
    List<ArrowBuf> buffers = recordBatch.getBuffers();
    recordBatch.close();
    for(ArrowBuf buf : buffers) {
      buf.release();
    }
  }

  void releaseValueVectors(List<ValueVector> valueVectors) {
    for(ValueVector valueVector : valueVectors) {
      valueVector.close();
    }
  }

  void generateData(DataAndVectorGenerator generator, int numRecords, ArrowBuf buffer) {
    for(int i = 0; i < numRecords; i++) {
      generator.writeData(buffer);
    }
  }

  private long generateDataAndEvaluate(DataAndVectorGenerator generator,
                                       NativeEvaluator evaluator,
                                       int numFields, int numExprs,
                                       int numRows, int maxRowsInBatch,
                                       int inputFieldSize)
    throws GandivaException, Exception {
    int numRemaining = numRows;
    List<ArrowBuf> inputData = new ArrayList<ArrowBuf>();
    List<ArrowFieldNode> fieldNodes = new ArrayList<ArrowFieldNode>();
    List<ValueVector> outputVectors = new ArrayList<ValueVector>();

    long start;
    long finish;
    long elapsedTime = 0;

    // set the bitmap
    while (numRemaining > 0) {
      int numRowsInBatch = maxRowsInBatch;
      if (numRowsInBatch > numRemaining) {
        numRowsInBatch = numRemaining;
      }

      // generate data
      for (int i = 0; i < numFields; i++) {
        ArrowBuf buf = allocator.buffer(numRowsInBatch * inputFieldSize);
        ArrowBuf validity = arrowBufWithAllValid(maxRowsInBatch);
        generateData(generator, numRowsInBatch, buf);

        fieldNodes.add(new ArrowFieldNode(numRowsInBatch, 0));
        inputData.add(validity);
        inputData.add(buf);
      }

      // create record batch
      ArrowRecordBatch recordBatch = new ArrowRecordBatch(numRowsInBatch, fieldNodes, inputData);

      // set up output vectors
      // for each expression, generate the output vector
      for (int i = 0; i < numExprs; i++) {
        ValueVector valueVector = generator.generateOutputVector(maxRowsInBatch);
        outputVectors.add(valueVector);
      }

      start = System.nanoTime();
      evaluator.evaluate(recordBatch, outputVectors);
      finish = System.nanoTime();

      elapsedTime += (finish - start);
      // fix numRemaining
      numRemaining -= numRowsInBatch;

      // release refs
      releaseRecordBatch(recordBatch);
      releaseValueVectors(outputVectors);

      inputData.clear();
      fieldNodes.clear();
      outputVectors.clear();
    }

    return (elapsedTime / MILLION);
  }

  long timedEvaluate(DataAndVectorGenerator generator,
                     Schema schema, List<ExpressionTree> exprs,
                     int numRows, int maxRowsInBatch,
                     int inputFieldSize)
  throws GandivaException, Exception {
    NativeEvaluator eval = NativeEvaluator.makeProjector(schema, exprs);

    try {
      return generateDataAndEvaluate(generator, eval,
              schema.getFields().size(), exprs.size(),
              numRows, maxRowsInBatch, inputFieldSize);
    } finally {
      eval.close();
    }
  }
}
