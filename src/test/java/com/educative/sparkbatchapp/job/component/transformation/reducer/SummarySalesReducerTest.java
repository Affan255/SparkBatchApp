package com.educative.sparkbatchapp.job.component.transformation.reducer;

import com.educative.sparkbatchapp.schema.SalesSummarySchema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SummarySalesReducerTest {

    private SummarySalesReducer reducerUnderTest;

    @BeforeEach
    public void init() {
        reducerUnderTest = new SummarySalesReducer();
    }

    @DisplayName("Test reduce rows for a seller's items sales")
    @Test
    public void testCallMethodReduceSalesForSeller() throws Exception {
        int itemSalesQuantity = 25;
        GenericRowWithSchema sampleRow = initializeRow(itemSalesQuantity);
        GenericRowWithSchema anotherSampleRow = initializeRow(itemSalesQuantity);
        Row expectedAggregatedRow = reducerUnderTest.call(sampleRow, anotherSampleRow);
        assertEquals(2*itemSalesQuantity, (Integer) expectedAggregatedRow.getAs(SalesSummarySchema.QUANTITY.name()));
    }

    public GenericRowWithSchema initializeRow(int itemSalesQuantity) {
        Object[] row = new Object[4];
        int index=0;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
        row[index++] = "Seller";
        row[index++] = Date.valueOf(LocalDate.parse("01-05-2022", formatter));
        row[index++] = "Product";
        row[index++] = itemSalesQuantity;
        return new GenericRowWithSchema(row, SalesSummarySchema.getSparkSchema());
    }

    @AfterEach
    public void tearDown() {
        reducerUnderTest = null;
    }
}
