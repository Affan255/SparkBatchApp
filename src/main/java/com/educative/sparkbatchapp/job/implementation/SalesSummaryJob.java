package com.educative.sparkbatchapp.job.implementation;

import com.educative.sparkbatchapp.job.Job;
import com.educative.sparkbatchapp.job.component.process.Processor;
import com.educative.sparkbatchapp.job.component.read.Reader;
import com.educative.sparkbatchapp.job.component.write.Writer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class SalesSummaryJob extends Job<Dataset<Row>> {

    private Reader salesSummaryReader;
    private Processor salesSummaryProcessor;
    private Writer salesSummaryWriter;

    @Autowired
    public SalesSummaryJob(@Qualifier("salesSummaryReader") Reader salesSummaryReader
    ,@Qualifier("salesSummaryProcessor") Processor salesSummaryProcessor
    ,@Qualifier("salesSummaryWriter") Writer salesSummaryWriter) {
        this.salesSummaryReader = salesSummaryReader;
        this.salesSummaryProcessor = salesSummaryProcessor;
        this.salesSummaryWriter = salesSummaryWriter;
    }

    @Override
    protected Dataset<Row> preProcess() {
        Dataset<Row> df = (Dataset<Row>)(salesSummaryReader.read());
        df.show();
        return df;
    }

    @Override
    protected Dataset<Row> process(Dataset<Row> preProcessOutput) {
        return (Dataset<Row>) salesSummaryProcessor.process(preProcessOutput);
    }

    @Override
    protected void postProcess(Dataset<Row> processOutput) {
        salesSummaryWriter.write(processOutput);
    }
}
