package com.educative.sparkbatchapp.job.implementation;

import com.educative.sparkbatchapp.job.context.IngesterJobContext;
import com.educative.sparkbatchapp.parameter.CommonJobParameter;
import com.educative.sparkbatchapp.parameter.IngesterJobParams;
import com.educative.sparkbatchapp.persistence.entity.JobConfiguration;
import com.educative.sparkbatchapp.persistence.entity.Sale;
import com.educative.sparkbatchapp.persistence.repository.ConfigRepository;
import com.educative.sparkbatchapp.persistence.repository.SalesRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(properties={"command.line.runner.enabled=false", "application.runner.enabled=false"})
@TestPropertySource(locations = "classpath:application-integrationtest.properties")
public class IngesterJobIntegrationTest {

    @Autowired
    private IngesterJob ingesterJob;

    @Autowired
    private IngesterJobContext ingesterJobContext;

    @Autowired
    private ConfigRepository configRepository;

    @Autowired
    private SalesRepository salesRepository;


    @Test
    @DisplayName("Integration test for Ingester Job")
    public void TestAndAssertIngesterJob() {
        String[] args = {"jobName=ingesterJob", "clientId=client1", "readFormat=json", "fileName=salestest"};
        loadParamsToContext(args);
        ingesterJob.execute();
        assertSalesIngested();
    }

    private void assertSalesIngested() {
        List<Sale> sales = salesRepository.findAll();
        assertEquals (11,sales.size());
    }

    private void loadParamsToContext(String[] args) {
        JobConfiguration jobConfiguration = loadJobConfig();
        configRepository.save(jobConfiguration);
        ingesterJobContext.getParameters().loadAllParams(args, CommonJobParameter.values());
        ingesterJobContext.getParameters().loadAllParams(args, IngesterJobParams.values());
    }

    public JobConfiguration loadJobConfig() {
        JobConfiguration jobConfiguration = new JobConfiguration();
        jobConfiguration.setClientId("client1");
        jobConfiguration.setConfigName("inputPath");
        jobConfiguration.setJobName("ingesterJob");
        jobConfiguration.setVal("src/test/resources/data/ingestion/");
        return jobConfiguration;
    }
}
