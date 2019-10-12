package ru.mail.polis.dao;

public final class TableToFlush {

    private final Table table;
    private final boolean poisonPill;

    public TableToFlush(final Table table) {
        this(table, false);
    }

    public TableToFlush(final Table table, final boolean poisonPill) {
        this.table = table;
        this.poisonPill = poisonPill;
    }

    public Table getTable() {
        return table;
    }

    public long getVersion() {
        return table.getVersion();
    }

    public boolean isPoisonPill() {
        return poisonPill;
    }
}
