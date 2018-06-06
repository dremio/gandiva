package org.apache.arrow.gandiva.expression;

import org.apache.arrow.gandiva.ipc.GandivaTypes;

public interface TreeNode {
    // TODO: Need to define the interface to serialize the node
    GandivaTypes.TreeNode toProtobuf() throws Exception;
}
