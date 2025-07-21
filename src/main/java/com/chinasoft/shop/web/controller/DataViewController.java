package com.chinasoft.shop.web.controller;

import com.chinasoft.shop.web.model.DailySales;
import com.chinasoft.shop.web.model.ProvinceCount;
import com.chinasoft.shop.web.repository.DailySalesRepository;
import com.chinasoft.shop.web.repository.ProvinceCountRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
@RequestMapping("/api/data")
public class DataViewController {

    @Autowired
    private ProvinceCountRepository provinceCountRepository;

    @Autowired
    private DailySalesRepository dailySalesRepository;

    @GetMapping("/province")
    public List<ProvinceCount> getProvinceData() {
        return provinceCountRepository.findAllByOrderByCountDesc();
    }

    @GetMapping("/sales")
    public List<DailySales> getSalesData() {
        return dailySalesRepository.findAllByOrderByDateAsc();
    }

    // --- 为未来扩展预留的接口 ---

    @GetMapping("/gender")
    public List<Map<String, Object>> getGenderData() {
        // TODO: 合作者需要实现从 'gender_distribution' 表读取数据
        // return genderRepository.findAll();
        // 返回模拟数据
        List<Map<String, Object>> result = new ArrayList<>();

        Map<String, Object> item1 = new HashMap<>();
        item1.put("name", "男性");
        item1.put("value", 1543);
        result.add(item1);

        Map<String, Object> item2 = new HashMap<>();
        item2.put("name", "女性");
        item2.put("value", 1287);
        result.add(item2);

        Map<String, Object> item3 = new HashMap<>();
        item3.put("name", "未知");
        item3.put("value", 320);
        result.add(item3);

        return Collections.unmodifiableList(result);
    }

    @GetMapping("/brand")
    public List<Map<String, Object>> getBrandData() {
        // TODO: 合作者需要实现从 'brand_sales' 表读取数据
        // return brandRepository.findTop10ByOrderByTotalSalesDesc();
        // 返回模拟数据
        List<Map<String, Object>> result = new ArrayList<>();

        Map<String, Object> item1 = new HashMap<>();
        item1.put("name", "品牌A");
        item1.put("value", 120500);
        result.add(item1);

        Map<String, Object> item2 = new HashMap<>();
        item2.put("name", "品牌B");
        item2.put("value", 98700);
        result.add(item2);

        Map<String, Object> item3 = new HashMap<>();
        item3.put("name", "品牌C");
        item3.put("value", 85600);
        result.add(item3);

        Map<String, Object> item4 = new HashMap<>();
        item4.put("name", "品牌D");
        item4.put("value", 76500);
        result.add(item4);

        Map<String, Object> item5 = new HashMap<>();
        item5.put("name", "品牌E");
        item5.put("value", 65400);
        result.add(item5);

        return Collections.unmodifiableList(result);
    }
}
