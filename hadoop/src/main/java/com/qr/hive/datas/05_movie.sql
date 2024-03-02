# explode 函数

# 创建表
create table movie_info(
    movie string,
    category string
)
row format delimited fields terminated by '\t';
# 加载数据
load data local inpath '/soft/hive/testFiles/movie.txt' into table movie_info;
# 查询
select
    movie,
    category_name
from movie_info
lateral view explode(split(category,',')) movie_info_tmp AS category_name;
