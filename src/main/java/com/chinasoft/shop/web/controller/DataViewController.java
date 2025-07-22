package com.chinasoft.shop.web.controller;

import com.chinasoft.shop.web.model.DailySales;
import com.chinasoft.shop.web.model.ProvinceCount;
// 导入新的 Repository
import com.chinasoft.shop.web.repository.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import scala.math.BigDecimal;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/data")
public class DataViewController {

    @Autowired
    private ProvinceCountRepository provinceCountRepository;

    @Autowired
    private DailySalesRepository dailySalesRepository;

//     --- 注入新增的 Repository ---
     @Autowired
     private AgeDistributionRepository ageDistributionRepository;

     @Autowired
     private CategorySalesRepository categorySalesRepository;

    @Autowired
    private BrandSalesRepository brandSalesRepository;

    @Autowired
    private GenderDistributionRepository genderDistributionRepository;

    @GetMapping("/province")
    public List<ProvinceCount> getProvinceData() {
        return provinceCountRepository.findAllByOrderByCountDesc();
    }

    @GetMapping("/sales")
    public List<DailySales> getSalesData() {
        return dailySalesRepository.findAllByOrderByDateAsc();
    }

    @GetMapping("/gender")
    public List<Map<String, Object>> getGenderData() {
        // TODO: 合作者需要实现从 'gender_distribution' 表读取数据
        return genderDistributionRepository.findAll().stream()
                .map(genderDist -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("name", genderDist.getGender());
                    map.put("value", genderDist.getCount());
                    return map;
                })
                .collect(Collectors.toList());
//        List<Map<String, Object>> result = new ArrayList<>();
//        Map<String, Object> item1 = new HashMap<>();
//        item1.put("name", "男性");
//        item1.put("value", 1543);
//        result.add(item1);
//
//        Map<String, Object> item2 = new HashMap<>();
//        item2.put("name", "女性");
//        item2.put("value", 1287);
//        result.add(item2);
//
//        Map<String, Object> item3 = new HashMap<>();
//        item3.put("name", "未知");
//        item3.put("value", 320);
//        result.add(item3);

//        return result;
    }

    @GetMapping("/brand")
    public List<Map<String, Object>> getBrandData() {
        return brandSalesRepository.findTop10ByOrderByTotalSalesDesc().stream()
                .map(b -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("name", b.getBrand());
                    map.put("value", b.getTotalSales());
                    return map;
                })
                .collect(Collectors.toList());
//        // TODO: 合作者需要实现从 'brand_sales' 表读取数据
//        List<Map<String, Object>> result = new ArrayList<>();
//
//        Map<String, Object> item1 = new HashMap<>();
//        item1.put("name", "品牌A");
//        item1.put("value", 120500);
//        result.add(item1);
//
//        Map<String, Object> item2 = new HashMap<>();
//        item2.put("name", "品牌B");
//        item2.put("value", 98700);
//        result.add(item2);
//
//        Map<String, Object> item3 = new HashMap<>();
//        item3.put("name", "品牌C");
//        item3.put("value", 85600);
//        result.add(item3);
//
//        Map<String, Object> item4 = new HashMap<>();
//        item4.put("name", "品牌D");
//        item4.put("value", 76500);
//        result.add(item4);
//
//        Map<String, Object> item5 = new HashMap<>();
//        item5.put("name", "品牌E");
//        item5.put("value", 65400);
//        result.add(item5);
//
//        return result;
    }

    // --- 新增API端点，提供模拟数据 ---

    @GetMapping("/age")
    public List<Map<String, Object>> getAgeData() {
        // TODO: 合作者需要实现从 'age_distribution' 表读取数据
        return ageDistributionRepository.findAll().stream()
                .map(ad -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("name", ad.getAgeGroup());
                    map.put("value", ad.getCount());
                    return map;
                })
                .collect(Collectors.toList());
        // 返回模拟数据
//        List<Map<String, Object>> result = new ArrayList<>();
//
//        Map<String, Object> item1 = new HashMap<>();
//        item1.put("name", "18岁以下");
//        item1.put("value", 350);
//        result.add(item1);
//
//        Map<String, Object> item2 = new HashMap<>();
//        item2.put("name", "18-24岁");
//        item2.put("value", 860);
//        result.add(item2);
//
//        Map<String, Object> item3 = new HashMap<>();
//        item3.put("name", "25-34岁");
//        item3.put("value", 1120);
//        result.add(item3);
//
//        Map<String, Object> item4 = new HashMap<>();
//        item4.put("name", "35-44岁");
//        item4.put("value", 640);
//        result.add(item4);
//
//        Map<String, Object> item5 = new HashMap<>();
//        item5.put("name", "45岁以上");
//        item5.put("value", 280);
//        result.add(item5);
//
//        return result;
    }

    @GetMapping("/category")
    public List<Map<String, Object>> getCategoryData() {
        // TODO: 合作者需要实现从 'category_sales' 表读取数据
        // 手动转换字段名，兼容Java 8，并将BigDecimal转为double
        return categorySalesRepository.findAllByOrderByTotalSalesDesc().stream()
                .map(catSales -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("name", catSales.getCategory());
                    // 将BigDecimal转换为double类型，保留两位小数
                    map.put("value", catSales.getTotalSales().setScale(2, RoundingMode.HALF_UP).doubleValue());
                    return map;
                })
                .collect(Collectors.toList());
        // 返回模拟数据
//        List<Map<String, Object>> result = new ArrayList<>();
//
//        Map<String, Object> item1 = new HashMap<>();
//        item1.put("name", "家用电器");
//        item1.put("value", 325400.50);
//        result.add(item1);
//
//        Map<String, Object> item2 = new HashMap<>();
//        item2.put("name", "手机数码");
//        item2.put("value", 289800.00);
//        result.add(item2);
//
//        Map<String, Object> item3 = new HashMap<>();
//        item3.put("name", "服装鞋包");
//        item3.put("value", 215600.80);
//        result.add(item3);
//
//        Map<String, Object> item4 = new HashMap<>();
//        item4.put("name", "美妆护肤");
//        item4.put("value", 189300.20);
//        result.add(item4);
//
//        Map<String, Object> item5 = new HashMap<>();
//        item5.put("name", "母婴用品");
//        item5.put("value", 152100.00);
//        result.add(item5);
//
//        Map<String, Object> item6 = new HashMap<>();
//        item6.put("name", "食品生鲜");
//        item6.put("value", 113500.60);
//        result.add(item6);
//
//        Map<String, Object> item7 = new HashMap<>();
//        item7.put("name", "图书音像");
//        item7.put("value", 87600.00);
//        result.add(item7);
//
//        return result;
    }
}    