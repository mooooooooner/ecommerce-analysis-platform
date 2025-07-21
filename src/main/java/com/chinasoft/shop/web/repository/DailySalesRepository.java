package com.chinasoft.shop.web.repository;

import com.chinasoft.shop.web.model.DailySales;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface DailySalesRepository extends JpaRepository<DailySales, String> {
    List<DailySales> findAllByOrderByDateAsc();
}
    