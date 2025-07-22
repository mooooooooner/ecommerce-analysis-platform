package com.chinasoft.shop.web.model;

import lombok.Data;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Entity
@Data
@Table(name = "brand_sales")
public class BrandSales {
    @Id
    private String brand;
    private BigDecimal totalSales;
    private Long totalCount;
}    