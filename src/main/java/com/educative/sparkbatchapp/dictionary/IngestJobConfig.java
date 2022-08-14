package com.educative.sparkbatchapp.dictionary;

public enum IngestJobConfig {
    INPUT_PATH("inputPath");

    private String configName;

    IngestJobConfig(String configName) {
        this.configName = configName;
    }

    public String getConfigName() {
        return configName;
    }
}
