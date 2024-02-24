create table personInfo (
    name string,
    fiends array<string>,
    children map<string,int>,
    address struct<street:string, city:string>
)
row format delimited
    fields terminated by ','
    collection items terminated by '_'
    map keys terminated by ':'
    lines terminated by '\n';



    row format delimited
fields terminated by ','
collection items terminated by '_'
map keys terminated by ':'
lines terminated by '\n';
