package com.educative.sparkbatchapp.persistence.repository;

import com.educative.sparkbatchapp.persistence.entity.Sale;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SalesRepository extends CrudRepository<Sale, Long> {
    List<Sale> findAll();
}
