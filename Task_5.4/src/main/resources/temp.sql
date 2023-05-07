
-------------------------  查询出增量数据 ----------------------------
select
    m.*
from
    $tempTable m
left join
    $hiveDb.$hiveTable h
on
    m.modified_time = h.modified_time
where
    h.modified_time is null


--------------------- 将数据写入到hive中 ---------------------------
insert into table $hiveDb.$hiveTable partition($partitionFiled=$partitionVal)
select * from add_mysql




-------------- 取出tables4 表中的 time1、time2 中的最大者-----------------------
select
        *
from
    table4
where
    (select max(time1) from table4) > (select max(time2) from table4)




---------------------- 建表 ------------------------
create table temp(
    id int,
    name string,
    dwd_insert_user string,
    dwd_insert_time string,
    dwd_modify_user string,
    dwd_modify_time string
)
partitioned by (day string)
row format delimited fields terminated by '\t';


---------------- 插入 ---------------------------------
insert into table temp partition(day = '20220101')
select deptno, dname, 'user1', date_format('2022-02-01','yyyy-MM-dd HH:mm:ss'), 'user1', date_format('2022-02-01','yyyy-MM-dd HH:mm:ss') from dept;


-----------------------
create table dept(
    deptno int, --部门编号
    dname string, --部门名称
    loc string --部门位置
)
partitioned by (etl_date string)  -- 声明day 为分区字段
row format delimited fields terminated by '\t';



alter table dept add partition (etl_date='20230403');
