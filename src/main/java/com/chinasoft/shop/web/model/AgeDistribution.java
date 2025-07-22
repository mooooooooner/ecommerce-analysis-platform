package com.chinasoft.shop.web.model;

import lombok.Data;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Data
@Table(name = "age_distribution")
public class AgeDistribution {
    @Id
    private String ageGroup; // 例如: "18-24岁"
    private Integer count;
}    