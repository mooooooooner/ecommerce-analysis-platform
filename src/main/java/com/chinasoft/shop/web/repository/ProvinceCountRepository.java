package com.chinasoft.shop.web.repository;

import com.chinasoft.shop.web.model.ProvinceCount;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface ProvinceCountRepository extends JpaRepository<ProvinceCount, String> {
    List<ProvinceCount> findAllByOrderByCountDesc();
}
    