

insert into table $odsDb.$odsTableName partition($partition=$partitionVal)
select * from $tempTableName


--------------------------------------
select
    m.*
from
    $tempTableName
left join
    (select max(orderkey) max_key) h
on
    1=1
where
    cast(m.ORDERKEY as int) > cast(h.orderkey as int)
and
    date_format(m.orderdate, 'yyyyMMdd') >= '19700504'



--------------- 删除分区 ----------------
alter table tableName drop partition ($partition=$partitionVal)

-- 插入数据  静态分区
insert [into overwrite] table $tableName partition($partition=$partitionVal)
select * from $tempTableName

