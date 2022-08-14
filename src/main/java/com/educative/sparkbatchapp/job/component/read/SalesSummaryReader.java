package com.educative.sparkbatchapp.job.component.read;

import com.educative.sparkbatchapp.configuration.SparkSessionProvider;
import com.educative.sparkbatchapp.dictionary.SalesSummaryQueries;
import com.educative.sparkbatchapp.job.context.CommonJobContext;
import com.educative.sparkbatchapp.job.context.SalesSummaryContext;
import com.educative.sparkbatchapp.parameter.SalesSummaryJobParams;
import com.educative.sparkbatchapp.persistence.manager.DatabaseConnManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class SalesSummaryReader implements Reader<Dataset<Row>> {

    @Autowired
    private SalesSummaryContext salesSummaryContext;

    private DatabaseConnManager connManager;

    @Autowired
    public SalesSummaryReader(DatabaseConnManager connManager) {
        this.connManager = connManager;
    }

    @Override
    public Dataset<Row> read() {
        String sellerIds = SalesSummaryQueries.buildINClause(salesSummaryContext.getParameters().getParamValue(SalesSummaryJobParams.SELLER_ID));
        String date = salesSummaryContext.getParameters().getParamValue(SalesSummaryJobParams.DATE);
        String product = salesSummaryContext.getParameters().getParamValue(SalesSummaryJobParams.PRODUCT);
        String salesRetrieveQuery = String.format(SalesSummaryQueries.SALES_PER_SELLER_ID_DATE_PROD,product,date,sellerIds);
        return getDataSetFromDb(salesRetrieveQuery);
    }

    public Dataset<Row> getDataSetFromDb(String query) {
        Properties properties = new Properties();
        properties.put("user", connManager.getUsername());
        properties.put("password", connManager.getPassword());
        return SparkSessionProvider.provideSession(salesSummaryContext.getSparkMode())
                .read().jdbc(connManager.getDbUrl(), "(" + query + ") sales_alias", properties);

    }
}
