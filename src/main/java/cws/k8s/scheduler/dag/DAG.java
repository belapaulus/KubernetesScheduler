package cws.k8s.scheduler.dag;

import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class DAG {

    private final Set<Vertex> vertices;
    private final Map<String, Vertex> verticesByLabel;
    private final Map<Long, Vertex> verticesByUid;
    private final Map<Integer, Edge> edgesByUid;

    public DAG() {
        this.vertices = new HashSet<>();
        this.verticesByLabel = new HashMap<>();
        this.verticesByUid = new HashMap<>();
        this.edgesByUid = new HashMap<>();
    }

    public boolean isEmpty() {
        return this.vertices.isEmpty();
    }

    public Vertex getVertexByLabel(String s) {
        // TODO concurrency
        final Vertex vertex = verticesByLabel.get(s);
        if (vertex == null) {
            throw new IllegalStateException("Vertex '" + s + "' can not be found! Vertices: " + verticesByLabel);
        }
        return vertex;
    }

    public void registerVertices(List<Vertex> vertices){
        synchronized (this.vertices) {
            for (Vertex vertex : vertices) {
                this.vertices.add(vertex);
                this.verticesByLabel.put(vertex.getLabel(), vertex);
                this.verticesByUid.put(vertex.getUid(), vertex);
            }
        }
    }

    public void registerEdges(List<Edge> edges) {
        synchronized (this.vertices) {
            for (Edge edge : edges) {
                Vertex parent = this.verticesByUid.get(edge.from);
                Vertex child = this.verticesByUid.get(edge.to);
                // log.info("Registering edge ({}, {}), from {} to {}", edge.from, edge.to, parent.label, child.label);
                parent.children.add(child);
                child.parents.add(parent);
                this.updateDescendants(parent, child);
                this.updateUpwardRank(parent, child);
                this.updateRCPE(parent, child);
            }
        }
        synchronized (this.edgesByUid) {
            for (Edge edge : edges) {
                this.edgesByUid.put(edge.uid, edge);
            }
        }
    }

    private void updateDescendants(Vertex vertex, Vertex child) {
        HashSet<Vertex> new_descendants = new HashSet<>();
        new_descendants.addAll(child.descendants);
        new_descendants.add(child);
        if (vertex.descendants.containsAll(new_descendants)) { return; }
        vertex.descendants.addAll(new_descendants);
        if (vertex.descendants.contains(vertex)) {
            throw new IllegalStateException(vertex.label);
        }
        for (Vertex parent: vertex.parents) {
            updateDescendants(parent, vertex);
        }

    }

    private void updateUpwardRank(Vertex vertex, Vertex newChild) {
        if (vertex.upwardRank > newChild.upwardRank) { return; }
        vertex.upwardRank = newChild.upwardRank+1;
        for (Vertex parent: vertex.parents) {
            updateUpwardRank(parent, vertex);
        }
    }

    private void updateRCPE(Vertex vertex, Vertex newChild) {
        if (vertex.rcpeRank > newChild.rcpeRank + (vertex.type == Type.RECURSE? 1 : 0)) { return; }
        if ((vertex.rcpeRank == newChild.rcpeRank + (vertex.type == Type.RECURSE? 1 : 0))
            && (vertex.rcpe.size() >= newChild.rcpe.size() + 1)) { return; }
        vertex.rcpe = (LinkedList<Vertex>) newChild.rcpe.clone();
        vertex.rcpe.addFirst(vertex);
        vertex.rcpeRank = newChild.rcpeRank + (vertex.type == Type.RECURSE? 1 : 0);
        for (Vertex parent: vertex.parents) {
            updateRCPE(parent, vertex);
        }
    }

    public void removeVertices(long... verticesIds) {
        throw new IllegalStateException();
    }

    public void removeEdges(int... edgesIds) {
        throw new IllegalStateException();
    }

}
