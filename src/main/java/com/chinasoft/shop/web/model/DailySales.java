package com.chinasoft.shop.web.model;

import lombok.Data;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Entity
@Data
@Table(name = "daily_sales")
public class DailySales {
    @Id
    private String date;
    private BigDecimal totalSales;
}
    