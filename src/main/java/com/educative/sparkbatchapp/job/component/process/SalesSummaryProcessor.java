package com.educative.sparkbatchapp.job.component.process;

import com.educative.sparkbatchapp.configuration.SparkSessionProvider;
import com.educative.sparkbatchapp.job.component.transformation.reducer.SummarySalesReducer;
import com.educative.sparkbatchapp.job.context.SalesSummaryContext;
import com.educative.sparkbatchapp.parameter.SalesSummaryJobParams;
import com.educative.sparkbatchapp.schema.SalesSummarySchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

@Component
public class SalesSummaryProcessor implements Processor<Dataset<Row>> {

    @Autowired
    private SalesSummaryContext salesSummaryContext;

    @Override
    public Dataset<Row> process(Dataset<Row> input) {
        String[] sellers = salesSummaryContext.getParameters().getParamValue(SalesSummaryJobParams.SELLER_ID).split(",");
        List<Row> results = Arrays.stream(sellers).map(seller -> {
            String sellerId = seller.replaceAll("'", "");
            Dataset<Row> sellerRows = input.filter(col(SalesSummarySchema.SELLER_ID.name()).equalTo(sellerId));
            return sellerRows.reduce(new SummarySalesReducer());
        }).collect(Collectors.toList());
        return SparkSessionProvider.provideSession(salesSummaryContext.getSparkMode())
                .createDataFrame(results, input.schema());
    }
}
