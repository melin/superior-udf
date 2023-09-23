create database test_db;
create temporary function get_idcard_info as 'com.superior.udf.hive.text.idcard.IdcardParserUDTF';
