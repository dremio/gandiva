package org.apache.arrow.gandiva.expression;

import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class IfNode implements TreeNode {
    IfNode(TreeNode condition, TreeNode ifNode, TreeNode elseNode, ArrowType retType) {
        this.condition = condition;
        this.ifNode = ifNode;
        this.elseNode = elseNode;
        this.retType = retType;
    }

    @Override
    public GandivaTypes.TreeNode toProtobuf() throws Exception {
        GandivaTypes.IfNode.Builder ifNodeBuilder = GandivaTypes.IfNode.newBuilder();
        ifNodeBuilder.setCond(condition.toProtobuf());
        ifNodeBuilder.setIfNode(ifNode.toProtobuf());
        if (elseNode != null) {
            ifNodeBuilder.setElseNode(elseNode.toProtobuf());
        }
        ifNodeBuilder.setReturnType(ArrowTypeHelper.ArrowTypeToProtobuf(retType));

        GandivaTypes.TreeNode.Builder builder = GandivaTypes.TreeNode.newBuilder();
        builder.setIfNode(ifNodeBuilder.build());
        return builder.build();
    }

    private TreeNode condition;
    private TreeNode ifNode;
    private TreeNode elseNode;
    private ArrowType retType;
}
