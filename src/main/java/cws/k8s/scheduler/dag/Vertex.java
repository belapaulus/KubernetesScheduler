package cws.k8s.scheduler.dag;

import lombok.Getter;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

@Getter
public class Vertex {
    //throw new IllegalArgumentException("No implementation for type: " + type + "(" + treeNode + ")");
    final String label;
    final long uid;
    final Type type;
    int upwardRank;
    Set<Vertex> parents;
    Set<Vertex> children;
    Set<Vertex> descendants;
    // repetitive critical path to exit (rcpe) and rcpeRank (the number of repetitive structures on the rcpe)
    LinkedList<Vertex> rcpe;
    int rcpeRank;

    Vertex(String label, long uid, Type type) {
        this.label = label;
        this.uid = uid;
        this.type = type;
        this.upwardRank = 0;
        this.parents = new HashSet<>();
        this.children = new HashSet<>();
        this.descendants = new HashSet<>();
        this.rcpe = new LinkedList<>();
        this.rcpe.add(this);
        this.rcpeRank = type == Type.RECURSE ? 1 : 0;
    }
}