package com.educative.sparkbatchapp.job.component.write;

public interface Writer<T> {
    void write(T input);
}
