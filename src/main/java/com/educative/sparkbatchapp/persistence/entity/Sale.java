package com.educative.sparkbatchapp.persistence.entity;

import javax.persistence.*;
import java.sql.Date;

@Entity
@Table(name = "SALES")
public class Sale {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "SELLER_ID")
    private String sellerId;
    @Column(name = "SALES_DATE")
    private Date salesDate;
    @Column(name = "PRODUCT")
    private String product;
    @Column(name = "QUANTITY")
    private Integer quantity;

    public Sale() {   }
    public Sale(String sellerId, Date salesDate, String product, Integer quantity) {
        this.sellerId = sellerId;
        this.salesDate = salesDate;
        this.product = product;
        this.quantity = quantity;
    }


}
