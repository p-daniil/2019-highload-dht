package ru.mail.polis.service;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

public class BasicTopology implements Topology<String> {

    private String[] nodes;
    private String me;

    public BasicTopology(String me, Set<String> nodes) {
        assert nodes.contains(me);
        this.me = me;
        this.nodes = new String[nodes.size()];
        nodes.toArray(this.nodes);
        Arrays.sort(this.nodes);
    }

    @Override
    public String primaryFor(ByteBuffer key) {
        final int hash = key.hashCode();
        final int n = (hash & Integer.MAX_VALUE) % nodes.length;
        return nodes[n];
    }

    @Override
    public boolean isMe(String node) {
        return node.equals(me);
    }

    @Override
    public Set<String> all() {
        return Set.of(nodes);
    }
}
