package org.apache.arrow.gandiva.expression;

import org.apache.arrow.vector.types.pojo.ArrowType;

public class IfNode implements TreeNode {
    IfNode(TreeNode condition, TreeNode ifNode, TreeNode elseNode, ArrowType retType) {
        this.condition = condition;
        this.ifNode = ifNode;
        this.elseNode = elseNode;
        this.retType = retType;
    }

    private TreeNode condition;
    private TreeNode ifNode;
    private TreeNode elseNode;
    private ArrowType retType;
}
