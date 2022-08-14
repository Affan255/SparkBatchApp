package com.educative.sparkbatchapp.job.component.transformation.reducer;

import com.educative.sparkbatchapp.schema.SalesSummarySchema;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

public class SummarySalesReducer implements ReduceFunction<Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SummarySalesReducer.class);

    @Override
    public Row call(Row aggregatedRow, Row currentRow) throws Exception {
        LOGGER.info("Reducing Sales per Seller and Product...");
        return addCurrentRowValues(aggregatedRow, currentRow);
    }

    public GenericRowWithSchema addCurrentRowValues(Row aggregatedRow, Row currentRow) {
        Object[] rowValues = new Object[4];
        int index=0;
        rowValues[index++] = currentRow.getAs("SELLER_ID");
        rowValues[index++] = currentRow.getAs("SALES_DATE");
        rowValues[index++] = currentRow.getAs("PRODUCT");
        rowValues[index++] = (Integer)currentRow.getAs("QUANTITY") + (Integer)aggregatedRow.getAs("QUANTITY");
         return new GenericRowWithSchema(rowValues, currentRow.schema());
    }
}
