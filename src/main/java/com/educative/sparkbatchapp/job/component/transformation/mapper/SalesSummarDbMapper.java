package com.educative.sparkbatchapp.job.component.transformation.mapper;

import com.educative.sparkbatchapp.dictionary.SalesSummaryQueries;
import com.educative.sparkbatchapp.persistence.manager.DatabaseConnManager;
import com.educative.sparkbatchapp.schema.SalesSummarySchema;
import net.bytebuddy.dynamic.scaffold.MethodRegistry;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

@Component
public class SalesSummarDbMapper implements MapPartitionsFunction<Row, Row> {

    private DatabaseConnManager connManager;

    @Autowired
    public SalesSummarDbMapper(DatabaseConnManager connManager) {
        this.connManager = connManager;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> iterator) throws Exception {
        return doRowsBatchInsert(iterator);
    }

    private Iterator<Row> doRowsBatchInsert(Iterator<Row> iterator) throws SQLException {
        try(Connection conn = connManager.getDataSource().getConnection();
            PreparedStatement statement = conn.prepareStatement(SalesSummaryQueries.INSERT_SALES_SUMMARY)) {
            while (iterator.hasNext()) {
                int index = 1;
                Row row = iterator.next();
                statement.setString(index++, row.getAs(SalesSummarySchema.SELLER_ID.name()));
                statement.setDate(index++, row.getAs(SalesSummarySchema.SALES_DATE.name()));
                statement.setString(index++, row.getAs(SalesSummarySchema.PRODUCT.name()));
                statement.setInt(index++, row.getAs(SalesSummarySchema.QUANTITY.name()));
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException sqlException) {
            throw sqlException;
        }
        return iterator;
    }
}
