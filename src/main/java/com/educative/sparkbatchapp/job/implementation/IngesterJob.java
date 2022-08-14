package com.educative.sparkbatchapp.job.implementation;

import com.educative.sparkbatchapp.job.Job;
import com.educative.sparkbatchapp.job.component.process.IngesterProcessor;
import com.educative.sparkbatchapp.job.component.process.Processor;
import com.educative.sparkbatchapp.job.component.read.IngesterReader;
import com.educative.sparkbatchapp.job.component.read.Reader;
import com.educative.sparkbatchapp.job.component.write.IngesterWriter;
import com.educative.sparkbatchapp.job.component.write.Writer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class IngesterJob extends Job<Dataset<Row>> {
    private static Logger LOGGER = LoggerFactory.getLogger(SparkJob.class);

    private Reader ingesterReader;
    private Processor ingesterProcessor;
    private Writer ingesterWriter;

    @Autowired
    public IngesterJob(@Qualifier("ingesterReader") Reader<Dataset<Row>> ingesterReader
                                    , @Qualifier("ingesterProcessor")Processor<Dataset<Row>> ingesterProcessor
                                    , @Qualifier("ingesterWriter")Writer<Dataset<Row>> ingesterWriter) {
        this.ingesterReader  = ingesterReader;
        this.ingesterProcessor = ingesterProcessor;
        this.ingesterWriter = ingesterWriter;
    }

    @Override
    protected Dataset<Row> preProcess() {
        return(Dataset<Row>) ingesterReader.read();
    }

    @Override
    protected Dataset<Row> process(Dataset<Row> preProcessOutput) {
        return (Dataset<Row>) ingesterProcessor.process(preProcessOutput);
    }

    @Override
    protected void postProcess(Dataset<Row> processOutput) {
        ingesterWriter.write(processOutput);
    }
}
