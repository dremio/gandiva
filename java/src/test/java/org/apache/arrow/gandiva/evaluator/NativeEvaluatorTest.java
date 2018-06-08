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
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class NativeEvaluatorTest {

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
}