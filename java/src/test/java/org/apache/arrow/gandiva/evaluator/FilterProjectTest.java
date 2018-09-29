package org.apache.arrow.gandiva.evaluator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.Condition;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.netty.buffer.ArrowBuf;

public class FilterProjectTest extends BaseEvaluatorTest {

  @Test
  public void testSimpleSV16() throws GandivaException, Exception {
    Field a = Field.nullable("a", int32);
    Field b = Field.nullable("b", int32);
    Field c = Field.nullable("c", int32);
    List<Field> args = Lists.newArrayList(a, b);

    Condition condition = TreeBuilder.makeCondition("less_than", args);

    Schema schema = new Schema(args);
    Filter filter = Filter.make(schema, condition);

    ExpressionTree expression = TreeBuilder.makeExpression("add", Lists.newArrayList(a, b), c);
    Projector projector = Projector.make(schema, Lists.newArrayList(expression));

    int numRows = 16;
    byte[] validity = new byte[]{(byte) 255, 0};
    // second half is "undefined"
    int[] values_a = new int[]{1, 2, 3, 4, 5, 6, 7, 8,  9, 10, 11, 12, 13, 14, 15, 16};
    int[] values_b = new int[]{2, 1, 4, 3, 6, 5, 8, 7, 10,  9, 12, 11, 14, 13, 14, 15};
    int[] expected = {3, 7, 11, 15};

    verifyTestCaseFor16(filter, projector, numRows, validity, values_a, values_b, expected);
  }

  private void verifyTestCaseFor16(Filter filter, Projector projector, int numRows, byte[] validity,
                              int[] values_a, int[] values_b, int[] expected) throws GandivaException {
    ArrowBuf validitya = buf(validity);
    ArrowBuf valuesa = intBuf(values_a);
    ArrowBuf validityb = buf(validity);
    ArrowBuf valuesb = intBuf(values_b);
    ArrowRecordBatch batch = new ArrowRecordBatch(
            numRows,
            Lists.newArrayList(new ArrowFieldNode(numRows, 0), new ArrowFieldNode(numRows, 0)),
            Lists.newArrayList(validitya, valuesa, validityb, valuesb));

    ArrowBuf selectionBuffer = buf(numRows * 2);
    SelectionVectorInt16 selectionVector = new SelectionVectorInt16(selectionBuffer);

    filter.evaluate(batch, selectionVector);

    IntVector intVector = new IntVector(EMPTY_SCHEMA_PATH, allocator);
    intVector.allocateNew(selectionVector.getRecordCount());

    List<ValueVector> output = new ArrayList<ValueVector>();
    output.add(intVector);
    projector.evaluate(batch, selectionVector, output);

    for (int i = 0; i < 4; i++) {
      assertFalse(intVector.isNull(i));
      assertEquals(expected[i], intVector.get(i));
    }

    // free buffers
    releaseRecordBatch(batch);
    releaseValueVectors(output);
    selectionBuffer.close();
    filter.close();
    projector.close();
  }
}
