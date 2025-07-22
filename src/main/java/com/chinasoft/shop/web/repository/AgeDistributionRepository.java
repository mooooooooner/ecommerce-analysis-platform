package com.chinasoft.shop.web.repository;

import com.chinasoft.shop.web.model.AgeDistribution;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AgeDistributionRepository extends JpaRepository<AgeDistribution, String> {
    // Spring Data JPA 提供了基础的 CRUD, 这里暂时不需要额外方法
}    