public class Test {


    public static void main(String[] args) throws Exception {
        // 创建 DatabaseReader 对象。指定 GeoLite2 数据库的位置
        File database = new File("/path/to/GeoLite2-City.mmdb");
        DatabaseReader dbReader = new DatabaseReader.Builder(database).build();

        InetAddress ipAddress = InetAddress.getByName("128.101.101.101");
        // 执行查询
        CityResponse response = dbReader.city(ipAddress);

        // 打印省市区信息
        String province = response.getCity().getNames().get("zh-CN"); // 中文名
        String city = response.getCity().getNames().get("en"); // 英文名
        String postalCode = response.getPostal().getCode(); // 邮编
        System.out.println("省市区信息：");
        System.out.println("省份（中文）：" + province);
        System.out.println("城市（英文）：" + city);
        System.out.println("邮编：" + postalCode);

        // 关闭 DatabaseReader
        dbReader.close();
    }
}
