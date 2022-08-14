package com.educative.sparkbatchapp.job.component.process;

public interface Processor<T> {
    T process(T input);
}
