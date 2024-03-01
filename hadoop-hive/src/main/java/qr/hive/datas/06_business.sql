# 窗口函数

#创建表
create table business(
    name string,
    orderdate string,
    cost int
)
row format delimited fields terminated by ',';

# 导入数据
load data local inpath '/soft/hive/testFiles/business.txt' into table business;

-- 1、需求：查询在2017年4月份购买过的顾客(name),及总人数（sum）
select
    name,
    count(name) over()
from business
where substring(orderdate,1,7) = '2017-04'
group by name;
-- 2、需求：查询顾客的购买明细及月购买总额
select
    name,
    orderdate,
    cost,
    sum(cost) over(partition by name, month(orderdate)) month_cost
from business;
-- 3、将每个顾客的cost按照日期进行累加
    -- ① 计算表business中的销费总额
    select
        name,
        orderdate,
        cost,
        sum(cost) over() sample
    from business;
    -- ② 计算每个人的销售总额
    select
        name,
        orderdate,
        cost,
        sum(cost) over(partition by name) sample
    from business;
    -- ③ 计算每个人截止到当天的消费总额
    select
        name,
        orderdate,
        cost,
        sum(cost) over(partition by name order by orderdate) sample
    from business
    -- ⑤ 计算每个人连续两天的消费总额
    select
        name,
        orderdate,
        cost,
        sum(cost) over(partition by name order by orderdate rows between 1 preceding and current row) sample
    from business;
    -- ⑥ 计算每个人从当前天到最后一天的消费总额
    select
        name,
        orderdate,
        cost,
        sum(cost) over(partition by name order by orderdate rows between current row and unbounded following) sampe
    from business;

-- 4）需求：查看顾客上次的购买时间
select
    name,
    orderdate,
    cost,
    lag(orderdate,1,'1900-01-01') over(partition by name order by orderdate) last_time
from business;

-- 5）需求：查询前20%时间的订单信息 把数据分成5分 按照日期排序 取第一份
select
    t1.name,
    t1.orderdate,
    t1.cost,
    t1.sorted
from(
    select
        name,
        orderdate,
        cost,
        ntile(5) over(order by orderdate) sorted
    from business
) t1
where t1.sorted = 1;
