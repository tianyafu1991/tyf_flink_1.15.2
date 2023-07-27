package com.tyf.flink.bean;

import java.io.Serializable;

public class CourseInfo implements Serializable {

    // 课程id
    private Integer id;

    // 课程名称
    private String name;

    // 类别id
    private Integer categoryId;

    // 描述信息
    private String description;

    // 课程价格
    private Double money;

    // 课程状态 1上架
    private Integer status;

    public CourseInfo() {
    }

    public CourseInfo(Integer id, String name, Integer categoryId, String description, Double money, Integer status) {
        this.id = id;
        this.name = name;
        this.categoryId = categoryId;
        this.description = description;
        this.money = money;
        this.status = status;
    }

    @Override
    public String toString() {
        return "CourseInfo{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", categoryId=" + categoryId +
                ", description='" + description + '\'' +
                ", money=" + money +
                ", status=" + status +
                '}';
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public Double getMoney() {
        return money;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }
}
