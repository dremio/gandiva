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

package org.apache.arrow.gandiva.expression;

import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;

public class FunctionNode implements TreeNode {
    FunctionNode(String function, List<TreeNode> children, ArrowType retType) {
        this.function = function;
        this.children = children;
        this.retType = retType;
    }

    @Override
    public GandivaTypes.TreeNode toProtobuf() throws Exception {
        GandivaTypes.FunctionNode.Builder fnNode = GandivaTypes.FunctionNode.newBuilder();
        fnNode.setFunctionName(function);
        fnNode.setReturnType(ArrowTypeHelper.ArrowTypeToProtobuf(retType));

        for(TreeNode arg : children) {
            fnNode.addInArgs(arg.toProtobuf());
        }

        GandivaTypes.TreeNode.Builder builder = GandivaTypes.TreeNode.newBuilder();
        builder.setFnNode(fnNode.build());
        return builder.build();
    }

    private String function;
    private List<TreeNode> children;
    private ArrowType retType;
}
