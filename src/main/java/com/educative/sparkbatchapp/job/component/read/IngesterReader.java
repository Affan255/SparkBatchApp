package com.educative.sparkbatchapp.job.component.read;

import com.educative.sparkbatchapp.configuration.SparkSessionProvider;
import com.educative.sparkbatchapp.dictionary.IngestJobConfig;
import com.educative.sparkbatchapp.job.context.CommonJobContext;
import com.educative.sparkbatchapp.job.context.IngesterJobContext;
import com.educative.sparkbatchapp.parameter.CommonJobParameter;
import com.educative.sparkbatchapp.parameter.IngesterJobParams;
import com.educative.sparkbatchapp.persistence.repository.ConfigRepository;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class IngesterReader implements Reader<Dataset<Row>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngesterReader.class);
    private static final String DOT = ".";

    private CommonJobContext jobContext;
    private ConfigRepository configRepository;

    @Autowired
    public IngesterReader(@Qualifier("ingesterJobContext") CommonJobContext jobContext, ConfigRepository configRepository) {
        this.jobContext = jobContext;
        this.configRepository = configRepository;
    }
    @Override
    public Dataset<Row> read() {
        SparkSession sparkSession = SparkSessionProvider.provideSession(jobContext.getSparkMode());
        String fileReadFormat = jobContext.getParameters().getParamValue(IngesterJobParams.READ_FORMAT);
        String fileName = jobContext.getParameters().getParamValue(IngesterJobParams.FILE_NAME);
        String fullPath = readInputPath().concat(fileName).concat(DOT).concat(fileReadFormat);
        DataFrameReader reader = sparkSession.read().format(fileReadFormat);
        LOGGER.info("Reading input file...");
        return getReaderForFormat(reader, fileReadFormat).load(fullPath);
    }

    private String readInputPath() {
        return configRepository.findTopByJobNameAndClientIdAndConfigName(jobContext.getParameters().getParamValue(CommonJobParameter.JOB_NAME)
                , jobContext.getParameters().getParamValue(CommonJobParameter.CLIENT_ID)
                , IngestJobConfig.INPUT_PATH.getConfigName()).getVal();
    }
    private DataFrameReader getReaderForFormat(DataFrameReader reader, String format) {
        if (format.equals(IngesterJobContext.JSON_FORMAT))
            return reader.option("multiline","true");
        else if (format.equals(IngesterJobContext.CSV_FORMAT))
            return reader.option("header","true");
        return reader;
    }
}
