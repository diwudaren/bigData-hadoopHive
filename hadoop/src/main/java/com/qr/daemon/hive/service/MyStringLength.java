package com.qr.daemon.hive.service;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * hive 自定义函数
 *  UDF 一对一
 *  UDAF 多对一
 *  UDTF 一对多
 * 自定义UDF函数，需要继承GenericUDF类
 * 需求: 计算指定字符串的长度
 * 1、添加pom文件、代码编写
 * 2、打包
 * 3、打成jar包上传到服务器                /soft/hive/myJar/myudf.jar
 * 4、将jar包添加到hive的classpath       【 hive (default)> add jar /soft/hive/myJar/myudf.jar; 】
 * 6、创建临时函数与开发好的java class关联  【 hive (default)> create temporary function my_len as "com.qr.hive.service.MyStringLength"; 】
 * 7、在hql中使用自定义的函数              【 hive (default)> select ename,my_len(ename) ename_len from emp; 】
 * 注意：因为add jar的方式本身也是临时生效，所以在创建永久函数的时候，需要执行路径（应且因为元数据的原因，这个路径还得是HDFS上的路径）
 * 8、创建永久函数                        create function my_len2 as "com.atguigu.hive.udf.MyUDF" using jar "hdfs://hadoop102:8020/udf/myudf.jar";
 * 9、删除永久函数                       【 hive (default)> drop function my_len2; 】
 *
 */
public class MyStringLength extends GenericUDF {


    /**
     * @param arguments 输入参数类型的鉴别器对象
     * @return 返回值类型的鉴别器对象
     * @throws UDFArgumentException 异常
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 判断输入参数的个数
        if(arguments.length !=1){
            throw new UDFArgumentLengthException("Input Args Length Error!!!");
        }
        // 判断输入参数的类型
        if(!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentTypeException(0,"Input Args Type Error!!!");
        }
        // 函数本身返回值为int，需要返回int类型的鉴别器对象
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 函数的逻辑处理
     * @param arguments 输入的参数
     * @return 返回
     * @throws HiveException 异常
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if(arguments[0].get() == null){
            return 0 ;
        }
        return arguments[0].get().toString().length();
    }


    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }
}
