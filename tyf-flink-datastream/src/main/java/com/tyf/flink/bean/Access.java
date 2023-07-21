package com.tyf.flink.bean;

public class Access {

    private Long time;

    private String name;

    private Double traffic;


    @Override
    public String toString() {
        return "Access{" +
                "time=" + time +
                ", name='" + name + '\'' +
                ", traffic=" + traffic +
                '}';
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getTraffic() {
        return traffic;
    }

    public void setTraffic(Double traffic) {
        this.traffic = traffic;
    }

    public Access() {
    }

    public Access(Long time, String name, Double traffic) {
        this.time = time;
        this.name = name;
        this.traffic = traffic;
    }


}
