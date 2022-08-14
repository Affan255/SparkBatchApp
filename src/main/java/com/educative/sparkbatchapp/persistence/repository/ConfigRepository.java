package com.educative.sparkbatchapp.persistence.repository;

import com.educative.sparkbatchapp.persistence.entity.JobConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ConfigRepository extends CrudRepository<JobConfiguration, Long> {
    JobConfiguration findTopByJobNameAndClientIdAndConfigName(String jobName, String clientId, String configName);

}
