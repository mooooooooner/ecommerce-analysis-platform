package com.chinasoft.shop.web.repository;

import com.chinasoft.shop.web.model.BrandSales;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface BrandSalesRepository extends JpaRepository<BrandSales, String> {
    List<BrandSales> findTop10ByOrderByTotalSalesDesc();
}    