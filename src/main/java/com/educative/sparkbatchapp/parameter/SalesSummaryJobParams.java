package com.educative.sparkbatchapp.parameter;

public enum SalesSummaryJobParams implements Parameter<SalesSummaryJobParams> {
    SELLER_ID("seller_id"),
    PRODUCT("product"),
    DATE("date");

    private String paramName;

    SalesSummaryJobParams(String paramName) {
        this.paramName = paramName;
    }

    @Override
    public String getParamName() {
        return paramName;
    }
}
