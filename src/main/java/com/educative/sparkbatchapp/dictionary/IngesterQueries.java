package com.educative.sparkbatchapp.dictionary;

public class IngesterQueries {
    public static final String SALES_TABLE = "SALES";
    public static final String INSERT_QUERY_SALES = "INSERT INTO " + SALES_TABLE + " (SELLER_ID, SALES_DATE, PRODUCT, QUANTITY) "
            + " VALUES(?, ?, ?, ?)";
}
