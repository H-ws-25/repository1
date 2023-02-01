-- 1. 准备新零售项目, MySQL数据, 充当业务数据源.
-- 直接运行 00_SOURCE\dump-yipin.sql 脚本文件即可.
-- 细节: 运行之前先设置, 该脚本文件的方言为 MySQL.


-- 2. 解决Hive表 字段中文解释 乱码问题.
-- 原因: Hive使用的是远程部署模式, Hive的元数据存储在MySQL的Hive数据库中,默认码表用的是latin1, 它不支持中文, 所以hive表 中文注释会乱码.
-- 解决: 运行如下的SQL语句, 修改 MySQL的 hive数据库中 元数据字段信息即可.
alter table hive.COLUMNS_V2 modify column COMMENT varchar(256) character set utf8;
alter table hive.TABLE_PARAMS modify column PARAM_VALUE varchar(4000) character set utf8;
alter table hive.PARTITION_PARAMS modify column PARAM_VALUE varchar(4000) character set utf8 ;
alter table hive.PARTITION_KEYS modify column PKEY_COMMENT varchar(4000) character set utf8;
alter table hive.INDEX_PARAMS modify column PARAM_VALUE varchar(4000) character set utf8;




-- 用于演示 全量覆盖的 源数据, 区域地址表.
select * from yipin.t_district;


-- 用于演示 增量导入(仅新增)的 源数据, 用户登陆记录表
select * from yipin.t_user_login;

-- 用于演示 增量导入(仅新增)的 源数据, 店铺表.
select * from yipin.t_store;









