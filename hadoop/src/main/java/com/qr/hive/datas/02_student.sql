create table if not exists student (
    id int,
    name string
)
row format delimited fields terminated by '\t'
stored as textfile
location '/user/hive/warehouse/student';

create table if not exists student2 as select id, name from student;
create table if not exists student3 like student;