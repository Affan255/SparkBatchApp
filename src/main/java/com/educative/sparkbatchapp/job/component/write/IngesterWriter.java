package com.educative.sparkbatchapp.job.component.write;

import com.educative.sparkbatchapp.job.component.transformation.mapper.IngesterSalesToDbMapper;
import com.educative.sparkbatchapp.persistence.manager.DatabaseConnManager;
import com.educative.sparkbatchapp.schema.SalesSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class IngesterWriter implements Writer<Dataset<Row>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngesterWriter.class);

    @Autowired
    private DatabaseConnManager connManager;

    @Override
    public void write(Dataset<Row> input) {
        input.show();
        LOGGER.info("Writing records to SALES table in DB...");
        Dataset<Row> persistedDf = input.mapPartitions(new IngesterSalesToDbMapper(connManager), RowEncoder.apply(SalesSchema.getSparkSchema()));
        persistedDf.count();
    }
}
