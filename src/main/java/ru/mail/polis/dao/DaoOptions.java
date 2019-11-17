package ru.mail.polis.dao;

public class DaoOptions {
    private double loadFactor = 0.1;
    private int compactionThreshold = 10;
    private SSTable.Impl ssTableImpl = SSTable.Impl.FILE_CHANNEL_READ;

    private DaoOptions() {
        // private constructor
    }

    public double getLoadFactor() {
        return loadFactor;
    }

    public int getCompactionThreshold() {
        return compactionThreshold;
    }

    public SSTable.Impl getSsTableImpl() {
        return ssTableImpl;
    }

    public static Builder builder() {
        return new DaoOptions().new Builder();
    }

    public class Builder {

        private Builder() {
            // private constructor
        }

        public Builder setLoadFactor(final double loadFactor) {
            DaoOptions.this.loadFactor = loadFactor;
            return this;
        }

        public Builder setCompactionThreshold(final int compactionThreshold) {
            DaoOptions.this.compactionThreshold = compactionThreshold;
            return this;
        }

        public Builder setSsTableImpl(final SSTable.Impl ssTableImpl) {
            DaoOptions.this.ssTableImpl = ssTableImpl;
            return this;
        }

        public DaoOptions build() {
            return DaoOptions.this;
        }

    }
}
