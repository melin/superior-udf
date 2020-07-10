create database test_db;
create table test_db.hello_world (name string, age string) partitioned by (day string) stored as orc;
create temporary function randomInt as 'com.dataworker.udf.number.GenericUDFRandomInt';
insert into test_db.hello_world partition(day='2017-05-19') (name, age) values ('zhangsan', '13');
