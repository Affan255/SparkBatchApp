package com.educative.sparkbatchapp.persistence.entity;

import javax.persistence.*;

@Entity
@Table(name = "JOB_CONFIG")
public class JobConfiguration {

    public JobConfiguration(){};

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "CLIENT_ID")
    private String clientId;
    @Column(name = "JOB_NAME")
    private String jobName;
    @Column(name = "CONFIG_NAME")
    private String configName;
    @Column(name = "VAL")
    private String val;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getConfigName() {
        return configName;
    }

    public void setConfigName(String configName) {
        this.configName = configName;
    }

    public String getVal() {
        return val;
    }

    public void setVal(String value) {
        this.val = value;
    }



}
