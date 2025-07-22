package com.chinasoft.shop.web.model;

import lombok.Data;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Data
@Table(name = "gender_distribution")
public class GenderDistribution {
    @Id
    private String gender;
    private Integer count;
}    