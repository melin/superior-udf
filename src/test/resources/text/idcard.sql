create database test_db;
create temporary function get_idcard_info as 'com.superior.udf.text.idcard.IdcardParserUDTF';
