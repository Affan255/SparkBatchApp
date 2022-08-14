package com.educative.sparkbatchapp.dictionary;

public class SalesSummaryQueries {
    public static final String SALES_TABLE = "SALES";
    public static final String SALES_SUMMARY_TABLE = "SALES_SUMMARY";
    public static final String STRING_TOKEN = "%s";
    public static final String SALES_PER_SELLER_ID_DATE_PROD = "SELECT SELLER_ID, SALES_DATE, PRODUCT, QUANTITY " +
            "FROM " + SALES_TABLE +  " WHERE PRODUCT='" + STRING_TOKEN + "' AND SALES_DATE='" +
            STRING_TOKEN + "'" + " AND SELLER_ID IN " + STRING_TOKEN;

    public static final String INSERT_SALES_SUMMARY = "INSERT INTO " + SALES_SUMMARY_TABLE +
            "(SELLER_ID, SALES_DATE, ITEM, TOTAL_QUANTITY)" + " VALUES(?,?,?,?)";

    public static String buildINClause(String commaSeparatedValues) {
        StringBuilder builder = new StringBuilder();
        String[] sellerIds = commaSeparatedValues.split(",");
        builder.append("(");
        for (int i=0;i<sellerIds.length;i++) {
            if (i== sellerIds.length-1)
                builder.append("'").append(sellerIds[i]).append("'").append(")");
            else
                builder.append("'").append(sellerIds[i]).append("'").append(",");
        }
        return builder.toString();
    }

}
