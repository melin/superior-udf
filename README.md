## 参考文档
1. [Hive- UDF&GenericUDF](http://www.jianshu.com/p/ca9dce6b5c37)
2. [HiveRunner](https://github.com/klarna/HiveRunner)

## DataCompute 自定义UDF函数
每创建一个UDF函数，需要创建一个UDF类，UDF类需要集成如下两个类中的一个：

1. org.apache.hadoop.hive.ql.exec.UDF
2. org.apache.hadoop.hive.ql.udf.generic.GenericUDF

### UDF
集成UDF类，在类中可以定义一个或者多个名称为evaluate的方法，方法可以定义0个或者多个参数，方法需要定义返回类型：
```java
public int evaluate(int a);}
public double evaluate(int a, double b);}
public String evaluate(String a, int b, Text c);}
public Text evaluate(String a);}
public String evaluate(List<Integer> a);} 
```
### UDF实例
定义一个md5函数，调用md5函数时，可以缺省第二个参数
```java
public class HashMd5UDF extends UDF {

    /**
     *
     * @param value
     * @return
     */
    public String evaluate(String value) {
        return Hashing.md5().hashString(value).toString();
    }

    /**
     *
     * @param value
     * @param charsetName
     * @return
     */
    public String evaluate(String value, String charsetName) {
        return Hashing.md5().hashString(value, Charset.forName(charsetName)).toString();
    }
}
```

### GenericUDF
适合需要处理数据库连接场景使用，例如：
```java
/**
 * http://lxw1234.com/archives/2015/08/454.htm
 *
 * Created by libinsong on 2017/3/9.
 */
public class HashMd5GenericUDF extends GenericUDF {

    private static final Logger LOGGER = LoggerFactory.getLogger(HashMd5GenericUDF.class);

    /**
     * 函数初始化操作，并定义函数的返回值类型；
     * 比如，在该方法中可以初始化对象实例，初始化数据库链接，初始化读取文件等；
     *
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        LOGGER.info("init...");
        //函数返回类型为String
        ObjectInspector returnType = PrimitiveObjectInspectorFactory
                .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
        return returnType;
    }

    /**
     * 函数处理的核心方法，用途和UDF中的evaluate一样；
     *
     * @param args
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if(args.length < 1 || args.length > 2)
            throw new IllegalArgumentException("参数个数不能小于1，大于三");

        String value = args[0].get().toString();
        String charsetName = null;
        if( args.length == 2) {
            charsetName = args[1].get().toString();
            return Hashing.md5().hashString(value, Charset.forName(charsetName)).toString();
        } else {
            return Hashing.md5().hashString(value).toString();
        }
    }

    /**
     * 显示函数的帮助信息
     *
     * @param children
     * @return
     */
    @Override
    public String getDisplayString(String[] children) {
        return "Usage: md5(String)";
    }

    /**
     * 任务完成后，执行关闭操作
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        LOGGER.info("close...");
    }
}
```

### 函数资源打包
使用maven-shade-plugin 打包，把引入的第三方jar合并打成一个jar包，但hive，hadoop 等jar不需要合并，避免生成的jar很大。
```shell
mvn clean package
```
