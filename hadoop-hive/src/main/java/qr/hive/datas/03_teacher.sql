create external table if not exists teacher(
    id int,
    name string
)
row format delimited
fields terminated by '\t'
stored as textfile
location '/school/teacher';

# 修改内外部表
alter table student set tblproperties('EXTERNAL'='TRUE');
# 修改表名称
alter table student rename to student2;
# 修改表字段 将列名id修改为student_id 类型不变
alter table student2 change column id id_bake int;
# 修改表字段 不修改列名 仅修改列的类型为string
alter table student2 change column id_bake id_bake string;
# 新增列 向表中新增一列 列名为gender 类型为string
alter table student2 add columns(gender string);
# 调整列的位置 现在想让name的列在最前面 做如下操作
alter table student2 change name name string first;
# 调整列的位置:将name更新到指定列的后面 操作如下
alter table student2 change name name string after id_bake;
# 删除表
drop table student2;
# 清除表中数据 Truncate 注意 Truncate只能删除管理表 不能删除外部表中数据
truncate table student2

                            # 数据导入 加载HDFS上的文件到hive表中 采用的类似剪切的方式 将文件拷贝到表的映射目录下
# 加载本地文件到hive
load data local inpath '本地文件' into table 表名
# 1 上传文件到HDFS 2 加载HDFS上数据
1 hadoop fs -put '本地文件' 'hdfs地址'
2 load data inpath 'hdfs文件' into table 表
# 加载数据覆盖表中已有的数据
load data local inpath '本地文件' overwrite into 表

# 向表中插入数据 Insert Insert into 以追加数据的方式插入到表或分区 原有数据不会删除  Insert overwrite 	会覆盖表中已存在的数据
insert into 表 select id,name from 表
# 查询语句中创建表并加载数据 As Select 简单没写案例
# 创建表时通过Location指定加载数据路径 location '文件路径'; 简单没写案例

                            # 数据导出 注意 insert导出时 hive会自动创建导出目录 但是由于是overwrite 所以导出路径一定要写准确 否则存在误删数据的可能
# Insert导出 local directory 1.将查询的结果导出到本地
insert overwrite local directory '导出的路径' select id, name from 表
# 2. 将查询的结果格式化导出到本地
insert  overwrite local directory  '导出的路径' row format delimited fields terminated by '\t' select id from 表
# 3. 将查询的结果导出到HDFS上(没有local) 格式化导出加 row format delimited fields terminated by '\t'
insert overwrite directory '导出的路径' select id, name from 表
                            # 数据迁移 export和import命令主要用于两个Hadoop平台集群之间Hive表迁移 (元数据源+真实数据)
# Export导出到HDFS上
export table 表 to '/导出到哪里地址'
# Import数据到指定Hive表中 注意: 先用export导出后, 再将数据导入
import table 表 from '/从哪里导入地址'

                            # 分区
# 查看分区
show partitions 表
# 增加分区
alter table 表 add partition(day='20240224')
# 同时添加多个分区
alter table 表 add partition(day='20240225') partition(day='20240226')
# 删除分区
alter table 表 drop partition(day='20240224')
# 同时删除多个分区
alter table 表 drop partition(day='20240225'), partition(day='20240226')
# 让分区表和数据产生关联的三种方式
    # 方式一: 上传数据后修复 执行修复命令
        '生成文件'
        msck repair table 表;
    # 方式二: 上传数据后添加分区 执行添加分区
        '生成文件'
        alter table 表 add partition(day='20240224')
    # 方式三: 创建文件夹后load数据到分区
        '生成文件'
        load data local inpath '文件' into table 表 partition(day='20240224');

# 分桶表 clustered by(id) into 4 buckets
create table student (id int, name string) clustered by(id) imto 4 buckets row format delimited fields terminated by '\t';





