package com.educative.sparkbatchapp.job.component.write;

import com.educative.sparkbatchapp.job.component.transformation.mapper.SalesSummarDbMapper;
import com.educative.sparkbatchapp.persistence.manager.DatabaseConnManager;
import com.educative.sparkbatchapp.schema.SalesSummarySchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SalesSummaryWriter implements Writer<Dataset<Row>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SalesSummaryWriter.class);
    private DatabaseConnManager connManager;

    @Autowired
    public SalesSummaryWriter(DatabaseConnManager connManager) {
        this.connManager = connManager;
    }

    @Override
    public void write(Dataset<Row> input) {
        Dataset<Row> processedDataset = input.mapPartitions(new SalesSummarDbMapper(connManager)
        , RowEncoder.apply(SalesSummarySchema.getSparkSchema()));
        LOGGER.info("Total Amount of Rows Processed in SalesSummaryWriter" + processedDataset.count());
    }
}
