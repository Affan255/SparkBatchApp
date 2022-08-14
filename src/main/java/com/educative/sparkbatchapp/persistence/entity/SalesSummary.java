package com.educative.sparkbatchapp.persistence.entity;

import javax.persistence.*;
import java.sql.Date;

@Entity
@Table(name = "SALES_SUMMARY")
public class SalesSummary {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "SELLER_ID")
    private String sellerId;
    @Column(name = "SALES_DATE")
    private Date salesDate;
    @Column(name = "ITEM")
    private String item;
    @Column(name = "TOTAL_QUANTITY")
    private Integer totalQuantity;

    public String getSellerId() {
        return sellerId;
    }

    public Date getSalesDate() {
        return salesDate;
    }

    public String getItem() {
        return item;
    }

    public Integer getTotalQuantity() {
        return totalQuantity;
    }
}
