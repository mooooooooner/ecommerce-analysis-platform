package com.chinasoft.shop.web.model;

import lombok.Data;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Data // Lombok注解，自动生成getter/setter等
@Table(name = "num_of_province")
public class ProvinceCount {
    @Id
    private String province;
    private Integer count;
}
    