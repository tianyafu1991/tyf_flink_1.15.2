create table student (
    id int primary key auto_increment comment '自增id'
    ,name varchar(200) comment '姓名'
    ,age int comment '年龄'
) comment '学生表' ;


insert into student(name,age) values('tyf',29),('zs',29),('ls',29),('ww',29);