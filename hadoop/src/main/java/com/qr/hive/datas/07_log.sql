# 主流文件存储格式对比实验
# 创建表 采用TextFile格式存储,--  文件大小为 18.1M
create table log_txt(
    track_time string,
    url string,
    session_id string,
    referer string,
    ip string,
    end_user_id string,
    city_id string
)
row format delimited fields terminated by '\t'
stored as textfile;
加载数据
load data local inpath '/soft/hive/testFiles/log.data' into table log_txt;
# hive (bigdata)> dfs -du -h /user/hive/warehouse/bigdata.db/log_txt; -- 文件大小为 18.1M

# 由于ORC格式时自带压缩的, 这设置orc存储不使用压缩
create table log_orc(
                        track_time string,
                        url string,
                        session_id string,
                        referer string,
                        ip string,
                        end_user_id string,
                        city_id string
)
    row format delimited fields terminated by '\t'
stored as orc
tblproperties("orc.compress"="NONE");
# 加载数据
insert into table log_orc select * from log_txt;
# 查看大小 -- 采用ORC（非压缩）格式存储，文件大小为7.7 M
hive (bigdata)> dfs -du -h /user/hive/warehouse/bigdata.db/log_orc;


# Parquet
create table log_parquet(
    track_time string,
    url string,
    session_id string,
    referer string,
    ip string,
    end_user_id string,
    city_id string
)
row format delimited fields terminated by '\t'
stored as parquet;
# 加载数据
insert into table log_parquet select * from log_txt;
# 查看大小  -- 文件大小为13.1 M
hive (bigdata)> dfs -du -h /user/hive/warehouse/bigdata.db/log_parquet;
# 存储文件的对比总结: ORC >  Parquet >  textFile

# 存储文件的查询速度测试
-- No rows affected (10.522 seconds)
insert overwrite local directory '/soft/hive/testFiles/input/log_txt' select substring(url,1,4) from log_txt;
-- No rows affected (11.495 seconds)
insert overwrite local directory '/soft/hive/testFiles/input/log_orc' select substring(url,1,4) from log_orc;
-- No rows affected (11.445 seconds)
insert overwrite local directory '/soft/hive/testFiles/input/log_parquet' select substring(url,1,4) from log_parquet;

# 测试存储和压缩
-- 创建表log_orc_zlib表，设置其使用ORC文件格式，并使用ZLIB压缩
create table log_orc_zlib(
    track_time string,
    url string,
    session_id string,
    referer string,
    ip string,
    end_user_id string,
    city_id string
)
row format delimited fields terminated by '\t'
stored as orc
tblproperties("orc.compress='ZLIB'");
-- 向表log_orc_zlib插入数据 -- 文件大小2.8 M
insert into table log_orc_zlib select * from log_txt;

-- 创建一个SNAPPY压缩的ORC存储方式
create table log_orc_snappy(
    track_time string,
    url string,
    session_id string,
    referer string,
    ip string,
    end_user_id string,
    city_id string
)
row format delimited fields terminated by '\t'
stored as orc
tblproperties("orc.compress"="SNAPPY");
-- 向表log_orc_snappy插入数据 -- 文件大小3.7 M
insert into log_orc_snappy select * from log_txt;


-- 创建一个SNAPPY压缩的parquet存储方式
create table log_parquet_snappy(
    track_time string,
    url string,
    session_id string,
    referer string,
    ip string,
    end_user_id string,
    city_id string
)
row format delimited fields terminated by '\t'
stored as parquet
tblproperties("parquet.compression"="SNAPPY");
-- 向表log_parquet_snappy插入数据 --文件大小6.4 MB
insert into log_parquet_snappy select * from log_txt;

-- 在实际的项目开发当中：
    --（1）hive表的数据存储格式一般选择：orc或parquet
    -- （2）压缩方式一般选择snappy，lzo。

