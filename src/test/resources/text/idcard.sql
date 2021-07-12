create database test_db;
create temporary function get_idcard_info as 'com.dataworker.udf.text.idcard.IdcardParserUDTF';
