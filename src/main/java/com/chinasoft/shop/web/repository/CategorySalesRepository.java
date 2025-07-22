package com.chinasoft.shop.web.repository;

import com.chinasoft.shop.web.model.CategorySales;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface CategorySalesRepository extends JpaRepository<CategorySales, String> {
    // 添加一个按销售额降序排序查询的方法，方便获取热门分类
    List<CategorySales> findAllByOrderByTotalSalesDesc();
}    