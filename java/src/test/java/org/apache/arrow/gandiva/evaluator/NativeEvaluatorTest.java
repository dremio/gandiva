/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.gandiva.evaluator;

import com.google.common.collect.Lists;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class NativeEvaluatorTest {

    private final static String EMPTY_SCHEMA_PATH = "";

    private BufferAllocator allocator;

    @Before
    public void init() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    ArrowBuf buf(byte[] bytes) {
        ArrowBuf buffer = allocator.buffer(bytes.length);
        buffer.writeBytes(bytes);
        return buffer;
    }

    ArrowBuf intBuf(int[] ints) {
        ArrowBuf buffer = allocator.buffer(ints.length * 4);
        for(int i = 0; i < ints.length; i++) {
            buffer.writeInt(ints[i]);
        }
        return buffer;
    }

    @Ignore
    @Test
    public void makeProjector() throws GandivaException {
        Field a = Field.nullable("a", new ArrowType.Int(64, true));
        Field b = Field.nullable("b", new ArrowType.Int(64, true));
        TreeNode aNode = TreeBuilder.makeField(a);
        TreeNode bNode = TreeBuilder.makeField(b);
        List<TreeNode> args = Lists.newArrayList(aNode, bNode);

        List<Field> cols = Lists.newArrayList(a, b);
        Schema schema = new Schema(cols);

        ArrowType retType = new ArrowType.Bool();
        TreeNode cond = TreeBuilder.makeFunction("greater_than", args, retType);
        TreeNode ifNode = TreeBuilder.makeIf(cond, aNode, bNode, retType);

        ExpressionTree expr = TreeBuilder.makeExpression(ifNode, Field.nullable("c", retType));
        List<ExpressionTree> exprs = Lists.newArrayList(expr);

        NativeEvaluator evaluator1 = NativeEvaluator.makeProjector(schema, exprs);
        NativeEvaluator evaluator2 = NativeEvaluator.makeProjector(schema, exprs);
        NativeEvaluator evaluator3 = NativeEvaluator.makeProjector(schema, exprs);

        evaluator1.close();
        evaluator2.close();
        evaluator3.close();
    }

    @Test
    public void testEvaluate() throws GandivaException, Exception {
        Field a = Field.nullable("a", new ArrowType.Int(32, true));
        Field b = Field.nullable("b", new ArrowType.Int(32, true));
        List<Field> args = Lists.newArrayList(a, b);

        Field retType = Field.nullable("c", new ArrowType.Int(32, true));
        ExpressionTree root = TreeBuilder.makeExpression("add", args, retType);

        List<ExpressionTree> exprs = Lists.newArrayList(root);

        Schema schema = new Schema(args);
        NativeEvaluator eval = NativeEvaluator.makeProjector(schema, exprs);

        int numRows = 16;
        byte[] validity = new byte[] {(byte) 255, 0};
        // second half is "undefined"
        int[] values_a = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
        int[] values_b = new int[] {16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};

        ArrowBuf validitya = buf(validity);
        ArrowBuf valuesa = intBuf(values_a);
        ArrowBuf validityb = buf(validity);
        ArrowBuf valuesb = intBuf(values_b);
        ArrowRecordBatch batch = new ArrowRecordBatch(
                numRows,
                Lists.newArrayList(new ArrowFieldNode(numRows, 8), new ArrowFieldNode(numRows, 8)),
                Lists.newArrayList(validitya, valuesa, validityb, valuesb));

        IntVector intVector = new IntVector(EMPTY_SCHEMA_PATH, allocator);
        intVector.allocateNew(numRows);

        List<ValueVector> output = new ArrayList<ValueVector>();
        output.add(intVector);
        eval.evaluate(batch, output);

        for(int i = 0; i < 8; i++) {
            assertFalse(intVector.isNull(i));
            assertEquals(17, intVector.get(i));
        }
        for(int i = 8; i < 16; i++) {
            assertTrue(intVector.isNull(i));
        }
        eval.close();
    }
}
