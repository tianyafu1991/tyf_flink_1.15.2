package com.tyf.flink.mock;

import com.tyf.flink.bean.CourseInfo;
import com.tyf.flink.bean.ProductAccess;

import java.util.*;

public class MockData {

    public ProductAccess generateProductAccess(Random rand,Map<Integer,List<CourseInfo>> courseInfoMap){
        Integer categoryId = rand.nextInt(2) + 1;

        String[] descriptions1 = new String[]{"pk哥的大数据运维实战课程","pk哥的云原生实战课程","pk哥的Flink实战课程","pk哥的Spark实战课程","pk哥的Hadoop实战课程"};
        String[] descriptions2 = new String[]{"pk哥的Java工程师实战课程","pk哥的Springboot实战课程","pk哥的SpringCloud实战课程"};
        String description = "";
        if(categoryId==1){
            description = descriptions1[rand.nextInt(descriptions1.length)];
        }else {
            description = descriptions2[rand.nextInt(descriptions2.length)];
        }



        return null;
    }

    public Map<Integer,List<CourseInfo>> generateCourseMap(){
        Map<Integer,List<CourseInfo>> courseInfoMap = new HashMap<>();
        List<CourseInfo> category1List = new ArrayList<>();
        List<CourseInfo> category2List = new ArrayList<>();


        courseInfoMap.put(1,category1List);
        courseInfoMap.put(2,category2List);

        return courseInfoMap;
    }

    public static void main(String[] args) {
        Random rand = new Random();

//        Map<Integer,CourseInfo> descriptionsMap2 = new HashMap<>();



        for (int i = 0; i < 100; i++) {
            System.out.println(rand.nextInt(2) + 1 );
        }

    }
}
