package com.educative.sparkbatchapp.job.component.transformation.flatmapper;

import com.educative.sparkbatchapp.schema.SalesSchema;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.springframework.stereotype.Component;
import scala.collection.mutable.WrappedArray;

import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

@Component
public class IngesterJsonFlatMapper implements FlatMapFunction<Row, Row> {

    private static final String SELLER_COLUMN = "Seller_Id";
    private static final String SALES_COLUMN = "Sales";

    @Override
    public Iterator<Row> call(Row row) throws Exception {
        return explodeRowsPerSeller(row).iterator();
    }

    private List<Row> explodeRowsPerSeller(Row inputRow) {
        List<Row> explodedRows = new LinkedList<>();
        String sellerId = inputRow.getAs(SELLER_COLUMN);
        WrappedArray<String> salesArray = inputRow.getAs(SALES_COLUMN);
        Row[] sales = (Row[]) salesArray.array();
        for(Row row: sales) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
            Date saleDate = Date.valueOf(LocalDate.parse(row.getAs("Date"),formatter));
            WrappedArray<String> itemsArray = row.getAs("Items");
            Row[] items = (Row[]) itemsArray.array();
            for (Row item: items) {
                String product = item.getAs("Product");
                long quantity = item.getAs("Quantity");
                Object[] rowValues = new Object[4];
                int valuesIndex = 0;
                rowValues[valuesIndex++] = sellerId;
                rowValues[valuesIndex++] = saleDate;
                rowValues[valuesIndex++] = product;
                rowValues[valuesIndex++] = quantity;
                explodedRows.add(new GenericRowWithSchema(rowValues, SalesSchema.getSparkSchema()));
            }
        }
        return explodedRows;
    }
}
