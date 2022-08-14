package com.educative.sparkbatchapp.job.implementation;

import com.educative.sparkbatchapp.job.context.SalesSummaryContext;
import com.educative.sparkbatchapp.parameter.CommonJobParameter;
import com.educative.sparkbatchapp.parameter.SalesSummaryJobParams;
import com.educative.sparkbatchapp.persistence.entity.Sale;
import com.educative.sparkbatchapp.persistence.entity.SalesSummary;
import com.educative.sparkbatchapp.persistence.repository.SalesRepository;
import com.educative.sparkbatchapp.persistence.repository.SalesSummaryRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.sql.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(properties = {"command.line.runner.enabled=false", "application.runner.enabled=false"})
@TestPropertySource(locations = "classpath:application-integrationtest.properties")
public class SalesSummaryJobIntegrationTest {

    @Autowired
    private SalesSummaryContext salesSummaryContext;

    @Autowired
    private SalesSummaryJob salesSummaryJob;

    @Autowired
    private SalesSummaryRepository salesSummaryRepository;

    @Autowired
    private SalesRepository salesRepository;

    @Test
    @DisplayName("Integration test for Sales Summary Job")
    public void TestAndAssertSalesSummaryJob() {
        String[] args = {"jobName=salesSummaryJob", "seller_id=Joe", "date=2020-11-11", "product=Food"};
        salesSummaryContext.getParameters().loadAllParams(args, SalesSummaryJobParams.values());
        salesSummaryContext.getParameters().loadAllParams(args, CommonJobParameter.values());
        loadSampleSales();
        salesSummaryJob.execute();
        assertSalesSummary();
    }

    private void assertSalesSummary() {
        List<SalesSummary> salesSummaryList = salesSummaryRepository.findAll();
        assertEquals(salesSummaryList.get(0).getTotalQuantity(),32);
    }

    private void loadSampleSales() {
        Sale sale1 = new Sale("Joe", Date.valueOf("2020-11-11"), "Food", 12);
        salesRepository.save(sale1);
        Sale sale2 = new Sale("Joe", Date.valueOf("2020-11-11"), "Food", 20);
        salesRepository.save(sale2);
    }
}
