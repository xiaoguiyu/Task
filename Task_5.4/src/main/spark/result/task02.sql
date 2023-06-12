
-- 查询ods层的增量数据
select * from $odsDb.$odsTableName


insert into table $dwdDb.$dwdTableName partition ($partition=$partitionVal)
select *, 'user1', '$date', 'user1', '$date' from $addDate














