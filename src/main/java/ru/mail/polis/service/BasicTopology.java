package ru.mail.polis.service;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class BasicTopology implements Topology<String> {

    private final String[] nodes;
    private final String me;

    /**
     * Basic implementation of cluster topology.
     *
     * @param me node, which has this instance of topology
     * @param nodes all nodes in cluster
     */
    public BasicTopology(final String me, final Set<String> nodes) {
        assert nodes.contains(me);
        this.me = me;
        this.nodes = new String[nodes.size()];
        nodes.toArray(this.nodes);
        Arrays.sort(this.nodes);
    }

    @Override
    public String primaryFor(final ByteBuffer key) {
        return nodes[primaryIndex(key)];
    }

    @Override
    public Set<String> primaryFor(ByteBuffer key, int from) {
        assert from <= nodes.length;
        final int n = primaryIndex(key);
        final Set<String> primaryNodes = new HashSet<>(from);
        for (int i = n, counter = from; counter > 0; i++, counter--) {
            primaryNodes.add(nodes[i % nodes.length]);
        }
        return primaryNodes;
    }

    private int primaryIndex(ByteBuffer key) {
        final int hash = key.hashCode();
        return (hash & Integer.MAX_VALUE) % nodes.length;
    }

    @Override
    public boolean isMe(final String node) {
        return node.equals(me);
    }

    @Override
    public Set<String> all() {
        return Set.of(nodes);
    }
}
