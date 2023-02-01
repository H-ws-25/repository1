--------------------------------hive 拉链表设计实现-------------------------------------------------------
--Step0: 切换数据库.
use test;

-- 查看表数据
show tables;
select * from test.dw_zipper;               -- 拉链表, 10条
select * from test.ods_zipper_update;       -- 增量采集信息表(记录的是新增数据 和 修改数据), 3条(1条修改, 2条新增)
select * from tmp_zipper;                   -- 临时表, 暂无数据.

--Step1：创建拉链表
create table dw_zipper(
      userid string,
      phone string,
      nick string,
      gender int,
      addr string,
      starttime string,
      endtime string
) row format delimited fields terminated by '\t';

--加载模拟数据
load data local inpath '/root/hivedata/zipper.txt' into table dw_zipper;

--使用put也可以
-- hadoop fs -put zipper.txt /user/hive/warehouse/test.db/dw_zipper

--查询
select userid,nick,addr,starttime,endtime from dw_zipper;


--Step2：模拟增量数据采集
create table ods_zipper_update(
    userid string,
    phone string,
    nick string,
    gender int,
    addr string,
    starttime string,
    endtime string
) row format delimited fields terminated by '\t';

load data local inpath '/root/hivedata/update.txt' into table ods_zipper_update;
-- 使用put也可以
-- hadoop fs -put update.txt /user/hive/warehouse/test.db/ods_zipper_update

select * from ods_zipper_update;

--Step3：创建临时表, 表结构, 字段必须和 拉链表保持一致.
create table tmp_zipper(
    userid string,
    phone string,
    nick string,
    gender int,
    addr string,
    starttime string,
    endtime string
) row format delimited fields terminated by '\t';

--Step4：合并拉链表与增量表
-- 核心细节: insert overwrite 临时表  select 增量采集信息表 union all (旧的拉链表 left join 增量采集信息表)
insert overwrite table tmp_zipper
select
    userid,
    phone,
    nick,
    gender,
    addr,
    starttime,
    endtime
from ods_zipper_update
union all
select
    a.userid,
    a.phone,
    a.nick,
    a.gender,
    a.addr,
    a.starttime,
    -- 如果不是交集, 或者 已经是历史数据了               就用原时间     否则就更改时间
    if(b.userid is null or a.endtime < '9999-12-31', a.endtime, date_sub(b.starttime, 1)) endtime
from dw_zipper a left join ods_zipper_update b on a.userid = b.userid order by userid;


--Step5：覆盖拉链表, 用临时表的结果, 全量覆盖 拉链表.
insert overwrite table dw_zipper select * from tmp_zipper;



-- step6: 查询最终结果
select * from dw_zipper;



-- 扩展: if()函数, 条件成立 走表达式1, 否则走表达式2,  格式:  if(判断条件, 表达式1, 表达式2)
select if(5 < 3, '成立', '不成立') hg;

select date_sub('2023-01-09', 1);