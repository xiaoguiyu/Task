

----- 查询增量数据
select
    m.*
from
    $tempTableName m
left join
   (select max(modified_time) as max_time from  $odsDb.$odsTableName) h
on
    1 =1
where
    m.modified_time > h.max_time

--- 保存增量数据

insert into table $odsDb.$odsTableName partition($partition=$partitionVal)
select * from $addTableData


----- table4 增量数据查询
select
    m.*
from
    $tempTableName m
left join
    (select max(greatest(time1, time2)) max_time from $odsDb.$odsTableName) h
on
    1 = 1
where
    m.time1 > h.max_time
or
    m.time2 > h.max_time












