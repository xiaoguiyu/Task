

select
    m.*
from
    $tempTableName m
left join
    $hiveDb.$hiveTableName h
on
    m.modified_time = h.modified_time
where
    h.modified_time is null


--------- 将数据写入到hive的ods层 -------------
insert into table $hiveDb.$hiveTableName partition($partition=$partitionVal)
select * from $tempTableName





------------------------------------- 创建表 ----------------------
create table $dwdTableName(
    $column
)
partition by ($partition string)



------------------- 插入数据 ----------------------
insert into table $dwdDb.$dwdTableName partition($partition)
select  $selectColumn, 'user1', '$date', 'user1', '$date'
    $partition  from temp
where $partition = '20230509'




(concat(orderdate," 00:00:00")
unix_timestamp(concat(orderdate," 00:00:00"))





-----------------------------------------
select
    m.*
from
    mysql_orders m
 join
    (select max(orderkey) as max_key ) h
on
    1 = 1
where
    case(m.ORDERKEY as int) > case(h.max_key as int)
and
     date_format(m.orderdate, 'yyyyMMdd') >= '19700101'









