-- 创建部门表dept
create table if not exists dept(
    deptno int, -- 部门编号
    dname string, -- 部门名称
    ioc int -- 部门位置
)
row format delimited
fields terminated by '\t'
stored as textfile
location '/user/hive/warehouse/dept';

-- 创建员工表
create table if not exists emp(
    empno int, -- 员工编号
    ename string, -- 员工名称
    job string, -- 员工岗位（大数据工程师、前端工程师、java工程师）
    sal double, -- 员工薪资
    deptno int -- 部门编号
)
row format delimited
fields terminated by '\t'
stored as textfile
location '/user/hive/warehouse/emp';

-- 等值Join
-- 内连接 只有进行连接的两个表中都存在与连接条件相匹配的数据才会被保留下来。
select
        t1.deptno,
        t2.deptno
    from emp t1
join dept t2 on t1.deptno = t2.deptno;
-- 左外连接 left JOIN操作符左边表中符合join子句的所有记录将会被返回。
select
    t1.deptno,
    t2.deptno
from emp t1
left join dept t2 on t1.deptno = t2.deptno;
-- 右外连接 right JOIN操作符右边表中符合join子句的所有记录将会被返回。
select
    t1.deptno,
    t2.deptno
from emp t1
right join dept t2 on t1.deptno = t2.deptno;
-- 满外连接 full 将会返回所有表中符合WHERE语句条件的所有记录。
select
    t1.deptno,
    t2.deptno
from emp t1
full join dept t2 on t1.deptno = t2.deptno;
-- 如果只想保留两个表各自特有数据，应该如何实现？


load data local inpath '/soft/hive/testFiles/student.txt' into table stu_bucket;
