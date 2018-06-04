package org.apache.arrow.gandiva.expression;

import org.apache.arrow.vector.types.pojo.ArrowType;
import java.util.List;

public class FunctionNode implements TreeNode {
    FunctionNode(String function, List<TreeNode> children, ArrowType retType) {
        this.function = function;
        this.children = children;
        this.retType = retType;
    }

    private String function;
    private List<TreeNode> children;
    private ArrowType retType;
}
