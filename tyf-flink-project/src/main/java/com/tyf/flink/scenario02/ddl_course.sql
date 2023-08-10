drop table if exists course;
create table if not exists course(
id int comment '课程id'
,name varchar(255) comment '课程name'
) engine=innodb charset utf8mb4 comment '课程表'
;



insert into course
values
(1,'pk哥Hadoop实战-new')
,(2,'pk哥Flink实战')
,(3,'pk哥Spark实战')
,(4,'pk哥云原生实战')
,(5,'pk哥大数据运维实战')
,(6,'pk哥Springboot实战')
,(7,'pk哥Java工程师实战')
,(8,'pk哥SpringCloud实战')
;