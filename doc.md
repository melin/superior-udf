
### 1. json_extract_value
#### 说明
UDTF 函数, 从json字符串数据中，获取key值，可以一次获取多个key的值返回

如果获取多个字段，优先json_extract_value函数，不要使用hive 自带的get_json_object，get_json_object每调用一次，需要解析一次，重复解析影响性能
``` sql
json_extract_value(jsondata, key1, ..., keyn) as (col1, ..., coln)
```
#### 实例
```sql
select a.name, b.name1, b.addr  from test_users_dt a 
lateral view json_extract_value('{"name":"melin", "address":"hangzhou"}', 'name', "address") b as name1, addr;

select json_extract_value('{"name":"melin", "address":{"name": "hangzhou"}}', "name", "address/name") as (name, address)
from test_users_dt

//解析json 数组
select json_extract_value('[{"name":"melin"}, {"name":"jack"}]', "/0/name", "/1/name") as (name1, name2)
from test_users_dt
```

### 2. json_extract_array_value
#### 说明
UDTF 函数, 从json字符串数据中，获取key值，可以一次获取多个key的值返回，类似json_extract_value，用于解析json 数组格式字符串，返回值类型为数组。

``` sql
json_extract_array_value(jsondata, key1, ..., keyn) as (col1, ..., coln)
```
#### 实例
```sql
select json_extract_array_value('[{"a": {"c": "val12"}}, {"a": {"c": "val2"}}]', "a/c") as (name)
```

### 3. explode_json_array
#### 说明
UDTF 函数, 从json字符串数组中获取key值，可以一次获取多个key的值返回，类似explode函数，数组值转为多行返回。

``` sql
explode_json_array(jsondata, key1, ..., keyn) as (col1, ..., coln)
```
#### 实例
```sql
select a.name, b.name1, b.addr  from test_users_dt a 
lateral view explode_json_array('[{"name":"melin", "address":"hangzhou"}, {"name":"lisi", "address":"hangzhou"}]', 'name', "address") b as name1, addr;

select explode_json_array('[{"name":"melin", "address":{"name": "hangzhou"}}]', "name", "address/name") as (name, address)
from test_users_dt

```
