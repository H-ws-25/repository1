-- 1. 创建数据库 yp_ods, 表示 新零售数仓的ods层, 主要用于临时存储数据的, 数据结构要和 MySQL源数据保持一致.
create database if not exists yp_ods;

-- 2. 查看所有数据库.
show databases;

-- 3. 切库.
use yp_ods;

-- 4. 查看该库下的 所有数据表.
show tables;

-- 场景1: 全量覆盖, 适用于 不更新或者少量更新的情况, 例如: 区域字典表.
-- 1. 在ods层, 创建 区域字典表, 该表的字段要和 MySQL数据库的业务源表字段保持一致.
create table yp_ods.t_district(
    id    string COMMENT '主键ID',
    code  string COMMENT '区域编码',
    name  string COMMENT '区域名称',
    pid   INT COMMENT '父级ID',
    alias string COMMENT '别名'
) comment '区域字典表'
row format delimited fields terminated by '\t'      -- 行字段分隔符: \t
stored as orc                                       -- orc存储格式(列存储)
tblproperties('orc.compress'='zlib');               -- 压缩格式: zlib(压缩比相对较高, 但是解压 和 压缩速度相对较慢)

-- 2. 通过Sqoop实现, 从MySQL 全量覆盖 导入数据到 hvie的该表(yp_ods.t_district)中.
-- 如下的代码是 Sqoop代码, 需要去 CRT中执行.
-- /usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
-- --connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
-- --username root \
-- --password 123456 \
-- --query "select * from t_district where 1=1 and  \$CONDITIONS" \
-- --hcatalog-database yp_ods \
-- --hcatalog-table t_district \
-- -m 1

-- 3. 查看表数据.
select * from yp_ods.t_district;




-- 场景2: 增量导入(仅新增), 适用于只有新增操作, 没有修改操作的需求, 例如: 用户访问日志记录表, 用户登陆日志记录表, 订单评价记录表...
-- 1. 在ods层, 创建 用户登陆记录表, 该表的字段要和 MySQL数据库的业务源表字段保持一致.
create table yp_ods.t_user_login(
    id          string ,
    login_user  string,
    login_type  string COMMENT '登录类型（登陆时使用）',
    client_id   string COMMENT '推送标示id(登录、第三方登录、注册、支付回调、给用户推送消息时使用)',
    login_time  string,
    login_ip    string,
    logout_time string
) comment '用户登陆记录表'
partitioned by (dt string)                          -- 分区字段
row format delimited fields terminated by '\t'      -- 行字段分隔符: \t
stored as orc                                       -- orc存储格式(列存储)
tblproperties('orc.compress'='zlib');               -- 压缩格式: zlib(压缩比相对较高, 但是解压 和 压缩速度相对较慢)

-- 2. 通过Sqoop实现, 从MySQL 全量覆盖 导入数据到 hvie的该表(yp_ods.t_district)中.
-- 如下的代码是 Sqoop代码, 需要去 CRT中执行.
-- 细节: 所有增量导入的第一步都是: 全量导入, 即: 把之前所有的数据一次性导入到Hive表中.
-- /usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
-- --connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
-- --username root \
-- --password 123456 \
-- --query "select *, '2023-01-08' as dt from t_user_login where 1=1 and  \$CONDITIONS" \
-- --hcatalog-database yp_ods \
-- --hcatalog-table t_user_login \
-- -m 1

-- 3. 如下的Sqoop语句, 可以实现 增量导入(仅新增)
-- 核心思路: 在query的SQL语句中做限定即可, 即: 查询出所有新增数据即可.
-- 小问题: 如何获取昨天的时间呢?  答案: 通过Linux命令即可实现, 例如:  my_dt = `date -d '1 days ago' +'%Y-%m-%d'`  即: 用变量 my_dt记录昨日时间.

-- /usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
-- --connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
-- --username root \
-- --password 123456 \
-- --query "select *, '2023-01-09' as dt from t_user_login where 1=1 and (login_time between '2023-01-09 00:00:00' and '2023-01-09 23:59:59')  and \$CONDITIONS" \
-- --hcatalog-database yp_ods \
-- --hcatalog-table t_user_login \
-- -m 1

-- 4. 查看表数据.
select * from yp_ods.t_user_login;





-- 场景3: 增量导入(新增 + 修改), 适用于既有新增, 也有更新数据, 例如: 店铺信息, 商品信息, 分类信息...  此处以店铺表举例.
-- 1. 在ods层, 创建 店铺表, 该表的字段要和 MySQL数据库的业务源表字段保持一致.
create table yp_ods.t_store(
    `id`                 string COMMENT '主键',
    `user_id`            string,
    `store_avatar`       string COMMENT '店铺头像',
    `address_info`       string COMMENT '店铺详细地址',
    `name`               string COMMENT '店铺名称',
    `store_phone`        string COMMENT '联系电话',
    `province_id`        INT COMMENT '店铺所在省份ID',
    `city_id`            INT COMMENT '店铺所在城市ID',
    `area_id`            INT COMMENT '店铺所在县ID',
    `mb_title_img`       string COMMENT '手机店铺 页头背景图',
    `store_description` string COMMENT '店铺描述',
    `notice`             string COMMENT '店铺公告',
    `is_pay_bond`        TINYINT COMMENT '是否有交过保证金 1：是0：否',
    `trade_area_id`      string COMMENT '归属商圈ID',
    `delivery_method`    TINYINT COMMENT '配送方式  1 ：自提 ；3 ：自提加配送均可; 2 : 商家配送',
    `origin_price`       DECIMAL,
    `free_price`         DECIMAL,
    `store_type`         INT COMMENT '店铺类型 22天街网店 23实体店 24直营店铺 33会员专区店',
    `store_label`        string COMMENT '店铺logo',
    `search_key`         string COMMENT '店铺搜索关键字',
    `end_time`           string COMMENT '营业结束时间',
    `start_time`         string COMMENT '营业开始时间',
    `operating_status`   TINYINT COMMENT '营业状态  0 ：未营业 ；1 ：正在营业',
    `create_user`        string,
    `create_time`        string,
    `update_user`        string,
    `update_time`        string,
    `is_valid`           TINYINT COMMENT '0关闭，1开启，3店铺申请中',
    `state`              string COMMENT '可使用的支付类型:MONEY金钱支付;CASHCOUPON现金券支付',
    `idCard`             string COMMENT '身份证',
    `deposit_amount`     DECIMAL(11,2) COMMENT '商圈认购费用总额',
    `delivery_config_id` string COMMENT '配送配置表关联ID',
    `aip_user_id`        string COMMENT '通联支付标识ID',
    `search_name`        string COMMENT '模糊搜索名称字段:名称_+真实名称',
    `automatic_order`    TINYINT COMMENT '是否开启自动接单功能 1：是  0 ：否',
    `is_primary`         TINYINT COMMENT '是否是总店 1: 是 2: 不是',
    `parent_store_id`    string COMMENT '父级店铺的id，只有当is_primary类型为2时有效'
)
comment '店铺表'
partitioned by (dt string)                          -- 分区字段
row format delimited fields terminated by '\t'      -- 行字段分隔符: \t
stored as orc                                       -- orc存储格式(列存储)
tblproperties('orc.compress'='zlib');               -- 压缩格式: zlib(压缩比相对较高, 但是解压 和 压缩速度相对较慢)

-- 2. 通过Sqoop实现, 从MySQL 全量覆盖 导入数据到 hvie的该表(yp_ods.t_district)中.
-- 如下的代码是 Sqoop代码, 需要去 CRT中执行.
-- 细节: 所有增量导入的第一步都是: 全量导入, 即: 把之前所有的数据一次性导入到Hive表中.

-- /usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
-- --connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
-- --username root \
-- --password 123456 \
-- --query "select *, '2023-01-08' as dt from t_store where 1=1 and (create_time between '2010-01-01 00:00:00' and '{my_dt} 23:59:59' or update_time between '2010-01-01 00:00:00' and '{my_dt} 23:59:59') and \$CONDITIONS" \
-- --hcatalog-database yp_ods \
-- --hcatalog-table t_store \
-- -m 1

-- 3. 如下的Sqoop语句, 可以实现 增量导入(新增 + 更新)
-- 核心思路: 在query的SQL语句中做限定即可, 即: 查询出所有新增数据即可.
-- 小问题: 如何获取昨天的时间呢?  答案: 通过Linux命令即可实现, 例如:  my_dt = `date -d '1 days ago' +'%Y-%m-%d'`  即: 用变量 my_dt记录昨日时间.
-- /usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
-- --connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
-- --username root \
-- --password 123456 \
-- --query "select *, '{my_dt}' as dt from t_store where 1=1 and (create_time between '${my_dt} 00:00:00' and '${my_dt} 23:59:59' or update_time between '${my_dt} 00:00:00' and '${my_dt} 23:59:59') and \$CONDITIONS" \
-- --hcatalog-database yp_ods \
-- --hcatalog-table t_store \
-- -m 1


-- 4. 查看表数据.
select * from yp_ods.t_store;


------------------------------- 总结 -------------------------------
-- 至此, 我们已经实现了 ods层的 三种导入方式的练习, 即: 全量覆盖, 增量导入(仅新增), 增量导入(新增+修改), 所以我们可以直接跑脚本了.
-- 即: 直接运行脚本, 完成ods层的搭建, 23张表, 表有数据.

-- Step1: 直接运行 01_ODS/create_ods_table.sql 脚本, 创建 ods层的数据表(23张)
-- 细节: 运行之前, 设置该脚本的方言为 hive sql

-- Step2: 把 01_ODS/sqoop_import.sh 这个脚本上传到Linux中, 例如: /root目录下, 然后执行该脚本, 实现: 从MySQL导入源数据到ods层的表中.
-- 细节: 因为时间不同, 所以记得把 sqoop_import.sh 脚本中的 2021-11-29 这个时间改为 2023-01-08, 然后再执行脚本.
-- 修改的快捷键: ctrl + r    (replace, 替换的意思)











