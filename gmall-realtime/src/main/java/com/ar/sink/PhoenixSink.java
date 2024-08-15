package com.ar.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.ar.entity.TableProcess;
import com.ar.utils.DruidDSUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;

public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    DruidDataSource druidDataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void close() throws Exception {
        druidDataSource.close();
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
        // 没来一条数据，从连接池获取一条可用的连接，这样可以避免长时间连接被服务器自动关闭的问题
        DruidPooledConnection conn = druidDataSource.getConnection();
        JSONObject data = value.f0;
        TableProcess tp = value.f1;
        // 1、 拼接sql，一定要有占位符
        StringBuffer sql = new StringBuffer();
        sql.append("upsert into ")
                .append(tp.getSinkTable())
                .append("(")
                .append(tp.getSinkColumns())
                .append(")values(")
                .append(tp.getSinkColumns().replaceAll("[^,]+", "?"))
                .append(")");
        System.out.println("插入语句：" + sql);
        // 2、 使用连接对象获取一个 PrepareStatement
        PreparedStatement ps = conn.prepareStatement(sql.toString());
        // 3、 给给占位符赋值
        String[] cs = tp.getSinkColumns().split(",");
        for (int i = 0; i < cs.length; i++) {
            Object v = data.get(cs[i]);
            String vv = v != null ? v.toString() : null;
            ps.setString(i + 1, vv);
        }

        // 4、 执行
        ps.execute();
        conn.commit();
        // 5、 关闭PrepareStatement
        ps.close();
        // 6、 归还连接给连接池
        conn.close();
    }
}
