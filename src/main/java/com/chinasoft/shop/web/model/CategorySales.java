package com.chinasoft.shop.web.model;

import lombok.Data;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Entity
@Data
@Table(name = "category_sales")
public class CategorySales {
    @Id
    private String category; // 商品类别
    private BigDecimal totalSales; // 总销售额
}    