create database test_db;
create table test_db.hello_world (json1 string, json2 string) partitioned by (day string) stored as orc;
create temporary function json_extract_array_value as 'com.dataworker.udf.json.GenericUDTFExplodeJsonArray';
insert into test_db.hello_world partition(day='2017-05-19') (json1, json2) values ('{"a":"2", "b":{"c": "1"}}', '{"b":"1"}');
