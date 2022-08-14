package com.educative.sparkbatchapp.persistence.repository;

import com.educative.sparkbatchapp.persistence.entity.SalesSummary;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SalesSummaryRepository extends CrudRepository<SalesSummary, Long> {
    List<SalesSummary> findAll();
}
