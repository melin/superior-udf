create database test_db;
create table test_db.hello_world (name string, age int, num bigint) stored as orc;
create temporary function sha512_encrypt as 'com.dataworker.udf.text.GenericUDFSha512Encrypt';
insert into test_db.hello_world values ("zhangsan", 18, 23123);
