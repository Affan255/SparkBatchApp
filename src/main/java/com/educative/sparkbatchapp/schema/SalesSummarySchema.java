package com.educative.sparkbatchapp.schema;

import org.apache.spark.sql.types.*;

public enum SalesSummarySchema {
    SELLER_ID("SELLER_ID", DataTypes.StringType, false, null),
    SALES_DATE("SALES_DATE", DataTypes.DateType, false, null),
    PRODUCT("PRODUCT", DataTypes.StringType, false, null),
    QUANTITY("QUANTITY", DataTypes.LongType, false, null);

    private String name;
    private DataType type;
    private boolean nullable;
    private Metadata metadata;

    SalesSummarySchema(String name, DataType type, boolean nullable, Metadata metadata ) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.metadata = metadata;
    }

    public static StructType getSparkSchema() {
        int schemaSize = values().length;
        StructField[] fields = new StructField[schemaSize];
        SalesSummarySchema[] schemaFields = values();
        for (int i=0;i<schemaSize;i++)
            fields[i] = new StructField(schemaFields[i].name, schemaFields[i].type, schemaFields[i].nullable, schemaFields[i].metadata);
        return new StructType(fields);
    }
}
