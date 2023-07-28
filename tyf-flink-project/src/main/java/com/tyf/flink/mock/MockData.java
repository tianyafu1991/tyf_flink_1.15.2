package com.tyf.flink.mock;

import com.alibaba.fastjson.JSON;
import com.tyf.flink.bean.CourseInfo;
import com.tyf.flink.bean.ProductAccess;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

public class MockData {

    public static final Random rand = new Random();

    /*
     * 随机生成国内IP地址
     */
    public static String mockIpStr() {

        // ip范围
        int[][] range = {{607649792, 608174079},// 36.56.0.0-36.63.255.255
                {1038614528, 1039007743},// 61.232.0.0-61.237.255.255
                {1783627776, 1784676351},// 106.80.0.0-106.95.255.255
                {2035023872, 2035154943},// 121.76.0.0-121.77.255.255
                {2078801920, 2079064063},// 123.232.0.0-123.235.255.255
                {-1950089216, -1948778497},// 139.196.0.0-139.215.255.255
                {-1425539072, -1425014785},// 171.8.0.0-171.15.255.255
                {-1236271104, -1235419137},// 182.80.0.0-182.92.255.255
                {-770113536, -768606209},// 210.25.0.0-210.47.255.255
                {-569376768, -564133889}, // 222.16.0.0-222.95.255.255
        };

        int index = rand.nextInt(10);
        String ip = num2ip(range[index][0] + new Random().nextInt(range[index][1] - range[index][0]));
        return ip;
    }

    /*
     * 将十进制转换成ip地址
     */
    public static String num2ip(int ip) {
        int[] b = new int[4];
        String x = "";

        b[0] = (int) ((ip >> 24) & 0xff);
        b[1] = (int) ((ip >> 16) & 0xff);
        b[2] = (int) ((ip >> 8) & 0xff);
        b[3] = (int) (ip & 0xff);
        x = Integer.toString(b[0]) + "." + Integer.toString(b[1]) + "." + Integer.toString(b[2]) + "." + Integer.toString(b[3]);

        return x;
    }

    public static ProductAccess generateProductAccess(Map<Integer,CourseInfo[]> courseInfoMap,String[] osArray){
        ProductAccess productAccess = new ProductAccess();

        Integer categoryId = rand.nextInt(2) + 1;

        CourseInfo[] courseInfos = courseInfoMap.get(categoryId);
        CourseInfo courseInfo = courseInfos[rand.nextInt(courseInfos.length)];

        productAccess.setCategoryId(courseInfo.getCategoryId());
        productAccess.setDescription(courseInfo.getDescription());
        productAccess.setId(courseInfo.getId());
        productAccess.setIp(mockIpStr());
        productAccess.setMoney(courseInfo.getMoney());
        productAccess.setName(courseInfo.getName());
        productAccess.setOs(osArray[rand.nextInt(osArray.length)]);
        productAccess.setStatus(courseInfo.getStatus());
        productAccess.setTs(System.currentTimeMillis());

        return productAccess;
    }

    public static Map<Integer, CourseInfo[]> generateCourseMap(){
        Map<Integer,CourseInfo[]> courseInfoMap = new HashMap<>();
        List<CourseInfo> category1List = new ArrayList<>();
        List<CourseInfo> category2List = new ArrayList<>();

        category1List.add(new CourseInfo(5,"pk哥大数据运维实战",1,"pk哥的大数据运维实战课程",8888.0D,1));
        category1List.add(new CourseInfo(4,"pk哥云原生实战",1,"pk哥的云原生实战课程",7777.0D,1));
        category1List.add(new CourseInfo(3,"pk哥Spark实战",1,"pk哥的Spark实战课程",6666.0D,1));
        category1List.add(new CourseInfo(2,"pk哥Flink实战",1,"pk哥的Flink实战课程",5555.0D,1));
        category1List.add(new CourseInfo(1,"pk哥Hadoop实战",1,"pk哥的Hadoop实战课程",4444.0D,1));

        category2List.add(new CourseInfo(7,"pk哥Java工程师实战",2,"pk哥的Java工程师实战课程",8888.0D,1));
        category2List.add(new CourseInfo(6,"pk哥Springboot实战",2,"pk哥的Springboot实战课程",7777.0D,1));
        category2List.add(new CourseInfo(8,"pk哥SpringCloud实战",2,"pk哥的SpringCloud实战课程",6666.0D,1));


        courseInfoMap.put(1,category1List.toArray(new CourseInfo[category1List.size()]));
        courseInfoMap.put(2,category2List.toArray(new CourseInfo[category2List.size()]));

        return courseInfoMap;
    }

    /**
     * 方法 2：使用 BufferedWriter 写文件
     * @param filepath 文件目录
     * @param content  待写入内容
     * @throws IOException
     */
    public static void bufferedWriterMethod(String filepath, String content) throws IOException {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filepath))) {
            bufferedWriter.write(content);
        }
    }

    public static void main(String[] args) throws Exception {
        Random rand = new Random();
        Map<Integer, CourseInfo[]> courseInfoMap = generateCourseMap();
        String[] osArray = new String[]{"Android","iOS","MAC OS","Harmony OS"};
        String outputPath = "data/productaccess.log";
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < 1000; i++) {
            ProductAccess productAccess = generateProductAccess(courseInfoMap, osArray);
            builder.append(JSON.toJSONString(productAccess)).append("\n");
        }

        bufferedWriterMethod(outputPath,builder.toString());

    }
}
