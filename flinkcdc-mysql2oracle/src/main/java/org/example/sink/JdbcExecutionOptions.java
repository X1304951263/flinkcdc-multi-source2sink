package org.example.sink;

import java.io.Serializable;

public class JdbcExecutionOptions implements Serializable {

    private static final long serialVersionUID = 1L;
    private int batchSize = 8096;
    private Long batchIntervalMs = 3000L;
    private int maxRetries = 3;

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public Long getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public void setBatchIntervalMs(Long batchIntervalMs) {
        this.batchIntervalMs = batchIntervalMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private int batchSize = 8096;
        private Long batchIntervalMs = 3000L;
        private int maxRetries = 3;

        public Builder setBatchSize(int batchSize){
            this.batchSize = batchSize;
            return this;
        }

        public Builder setBatchIntervalMs(Long batchIntervalMs){
            this.batchIntervalMs = batchIntervalMs;
            return this;
        }

        public Builder setMaxRetries(int maxRetries){
            this.maxRetries = maxRetries;
            return this;
        }

        public JdbcExecutionOptions build() {
            JdbcExecutionOptions options = new JdbcExecutionOptions();
            options.setBatchSize(batchSize);
            options.setBatchIntervalMs(batchIntervalMs);
            options.setMaxRetries(maxRetries);
            return options;
        }
    }

}
