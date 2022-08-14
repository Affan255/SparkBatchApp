package com.educative.sparkbatchapp.job.component.process;

import com.educative.sparkbatchapp.job.component.transformation.flatmapper.IngesterJsonFlatMapper;
import com.educative.sparkbatchapp.schema.SalesSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class IngesterProcessor implements Processor<Dataset<Row>> {
    private static Logger LOGGER = LoggerFactory.getLogger(IngesterProcessor.class);
    @Override
    public Dataset<Row> process(Dataset<Row> input) {
        LOGGER.info("Flattening JSON records...");
        Dataset<Row> parsedResults = input.flatMap(new IngesterJsonFlatMapper(), RowEncoder.apply(SalesSchema.getSparkSchema()));
        return parsedResults;
    }
}
