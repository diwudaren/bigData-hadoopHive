package com.qr.daemon.flume.service;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;


import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class EntityToMapConverter {

    public static Map<String, Object> entityToMap(Object entity) throws IllegalAccessException {
        Map<String, Object> map = new HashMap<>();
        Class<?> clazz = entity.getClass();
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true); // 使得私有字段也可以访问
            if (field.getType().getName().contains("com.qr.flume.service")){
                Map<String, Object> fieldMap = entityToMap(field.getName());
                map.putAll(fieldMap);
                continue;
            }
            map.put(field.getName(), field.get(entity));
        }
        return map;
    }

    // 示例实体类
    public static class User {
        private String name;
        private int age;

        // 构造函数、getter和setter省略

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public User(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }

    // 示例实体类
    public static class Order {
        private String orderId;
        private double amount;

        // 构造函数、getter和setter省略

        public String getOrderId() {
            return orderId;
        }

        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }

        public double getAmount() {
            return amount;
        }

        public void setAmount(double amount) {
            this.amount = amount;
        }

        public Order(String orderId, double amount) {
            this.orderId = orderId;
            this.amount = amount;
        }
    }

    // 包含多个实体的对象
    public static class UserOrder {
        private User user;
        private Order order;

        // 构造函数、getter和setter省略

        public User getUser() {
            return user;
        }

        public void setUser(User user) {
            this.user = user;
        }

        public Order getOrder() {
            return order;
        }

        public void setOrder(Order order) {
            this.order = order;
        }


    }

    public static void main(String[] args) throws IllegalAccessException, IOException {
        UserOrder userOrder = new UserOrder();
        userOrder.setUser(new User("Alice", 30));
        userOrder.setOrder(new Order("12345", 99.99));

        String jsonString = JSONObject.toJSONString(userOrder);
        Gson gson = new Gson();
        Map<String, Object> map = gson.fromJson(jsonString, Map.class);
        Map<String, Object> orderMap = (Map<String, Object>) map.get("order");
        map.putAll(orderMap);
        Map<String, Object> userMap = (Map<String, Object>) map.get("user");
        map.putAll(userMap);
        System.out.println(map);
    }
}