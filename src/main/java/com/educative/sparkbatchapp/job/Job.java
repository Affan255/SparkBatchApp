package com.educative.sparkbatchapp.job;

public abstract class Job<T> {
    abstract protected T preProcess();
    abstract protected T process(T preProcessOutput);
    abstract protected void postProcess(T processOutput);

    public void execute() {
        T preProcessOutput = preProcess();
        T processOutput = process(preProcessOutput);
        postProcess(processOutput);
    }
}
