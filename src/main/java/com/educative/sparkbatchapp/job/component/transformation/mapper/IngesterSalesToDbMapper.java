package com.educative.sparkbatchapp.job.component.transformation.mapper;

import com.educative.sparkbatchapp.dictionary.IngesterQueries;
import com.educative.sparkbatchapp.persistence.manager.DatabaseConnManager;
import com.educative.sparkbatchapp.schema.SalesSchema;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public class IngesterSalesToDbMapper implements MapPartitionsFunction<Row, Row> {

    private DatabaseConnManager connManager;

    @Autowired
    public IngesterSalesToDbMapper(DatabaseConnManager connManager) {
        this.connManager = connManager;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> iterator) throws Exception {
        return doRowsBatchInsert(iterator);
    }

    private Iterator<Row> doRowsBatchInsert(Iterator<Row> iterator) throws SQLException {
        try(Connection conn = connManager.getDataSource().getConnection();
            PreparedStatement statement = conn.prepareStatement(IngesterQueries.INSERT_QUERY_SALES)) {
            while (iterator.hasNext()) {
                int index = 1;
                Row row = iterator.next();
                System.out.println(row);
                statement.setString(index++, row.getAs(SalesSchema.SELLER_ID.name()));
                statement.setDate(index++, row.getAs(SalesSchema.DATE.name()));
                statement.setString(index++, row.getAs(SalesSchema.PRODUCT.name()));
                statement.setLong(index++, row.getAs(SalesSchema.QUANTITY.name()));
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException sqlException){
            throw sqlException;
        }
        return iterator;
    }
}
