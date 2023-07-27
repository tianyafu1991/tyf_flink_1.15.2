package com.tyf.flink.bean;

import java.io.Serializable;

public class ProductAccess implements Serializable {
    // 类别id
    private Integer categoryId;
    // 描述信息
    private String description;
    // 课程id
    private Integer id;
    // 用户访问的ip
    private String ip;
    // 课程价格
    private Double money;
    // 课程名称
    private String name;
    // 用户的操作系统
    private String os;
    // 课程状态 1上架
    private Integer status;
    // 日志访问时间
    private Long ts;

    public ProductAccess() {
    }

    public Integer getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Integer categoryId) {
        this.categoryId = categoryId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Double getMoney() {
        return money;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "ProductAccess{" +
                "categoryId=" + categoryId +
                ", description='" + description + '\'' +
                ", id=" + id +
                ", ip='" + ip + '\'' +
                ", money=" + money +
                ", name='" + name + '\'' +
                ", os='" + os + '\'' +
                ", status=" + status +
                ", ts=" + ts +
                '}';
    }
}
