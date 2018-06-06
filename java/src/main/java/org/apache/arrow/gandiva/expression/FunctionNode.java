package org.apache.arrow.gandiva.expression;

import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.Iterator;
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

        Iterator<TreeNode> it = children.listIterator();
        while (it.hasNext()) {
            TreeNode arg = it.next();
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
