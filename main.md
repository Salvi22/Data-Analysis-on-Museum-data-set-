```python
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StringType, ArrayType, DoubleType
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as func
import re
from pyspark import SparkContext
```


```python
SparkContext.setSystemProperty('spark.executor.memory', '4g')
sc = SparkContext("local","spark")
spark1 = SparkSession.builder.appName('spark').getOrCreate()
data_frame = spark1.read.csv('MetObjects.csv',inferSchema=True,header=True)
```


```python
dataframe_1= data_frame.select("Object ID","Dimensions")
type(dataframe_1)
```




    pyspark.sql.dataframe.DataFrame




```python
dataframe_1.sample(withReplacement=False, fraction=0.01).show(truncate=False)
```

    +---------+----------------------------------------------------------------------------------------------------------+
    |Object ID|Dimensions                                                                                                |
    +---------+----------------------------------------------------------------------------------------------------------+
    |348      |H. 4 1/4 in. (10.8 cm); Diam. 13 1/2 in. (34.3 cm)                                                        |
    |549      |Dimensions unavailable                                                                                    |
    |778      |3 3/8 x 9 3/4 x 19 1/4 in. (8.6 x 24.8 x 48.9 cm)                                                         |
    |null     |null                                                                                                      |
    |872      |Diam. 6 1/4 in. (15.9 cm)                                                                                 |
    |907      |3/4 x 3 1/4 x 3 1/4 in. (1.9 x 8.3 x 8.3 cm)                                                              |
    |969      |H. 1 7/8 in. (4.8 cm); Diam. 1 3/8 in. (3.5 cm)                                                           |
    |997      |Diam. 6 1/4 in. (15.9 cm)                                                                                 |
    |1001     |Diam. 6 1/4 in. (15.9 cm)                                                                                 |
    |1029     |33 1/2 x 14 x 11 5/16 in. (85.1 x 35.6 x 28.7 cm); Diam. 11 5/16 in. (28.7cm); 452 oz. 16 dwt. (14084.2 g)|
    |1103     |Dimensions unavailable                                                                                    |
    |1207     |H. 7 3/8 in. (18.7 cm)                                                                                    |
    |1301     |7 7/8 x 5 1/8 x 5 1/8 in. (20 x 13 x 13 cm)                                                               |
    |1477     |H. 5 in. (12.7 cm)                                                                                        |
    |1652     |33 1/2 x 17 1/2 x 21 in. (85.1 x 44.5 x 53.3 cm)                                                          |
    |1672     |31 1/2 x 15 3/8 x 19 1/4 in. (80 x 39.1 x 48.9 cm)                                                        |
    |1955     |H. 47 in. (119.4 cm); Diam. 22 1/4 in. (56.5 cm)                                                          |
    |2016     |30 1/2 x 33 3/4 x 20 in. (77.5 x 85.7 x 50.8 cm)                                                          |
    |2025     |79 5/8 x 43 1/8 x 21 7/8 in. (202.2 x 109.5 x 55.6 cm)                                                    |
    |2112     |Cup: H. 2 1/2 in. (6.4 cm); Diam. 2 3/4 in. (6.4 x 7 cm)                                                  |
    +---------+----------------------------------------------------------------------------------------------------------+
    only showing top 20 rows
    
    


```python
dataframe_2=dataframe_1.filter(dataframe_1.Dimensions.contains('cm'))
type(dataframe_2)
```




    pyspark.sql.dataframe.DataFrame




```python
@udf(returnType=StringType())
def dimensions_in_cm(s):
     q=s[s.find("(")+1:s.find(")")]
     return q
```


```python
type(dataframe_2["Dimensions"][1])
```




    pyspark.sql.column.Column




```python
dataframe_3 = dataframe_2.withColumn("dimensions_in_cm", dimensions_in_cm(col("Dimensions")))
dataframe_3.select("dimensions_in_cm").show(20, truncate=False)
```

    +----------------+
    |dimensions_in_cm|
    +----------------+
    |1.7 cm          |
    |1.7 cm          |
    |1.7 cm          |
    |1.7 cm          |
    |1.7 cm          |
    |1.7 cm          |
    |1.7 cm          |
    |1.3 cm          |
    |2.9 cm          |
    |2.9 cm          |
    |2.9 cm          |
    |2.9 cm          |
    |2.9 cm          |
    |2.9 cm          |
    |1.3 cm          |
    |1.3 cm          |
    |1.9 cm          |
    |1.9 cm          |
    |1.9 cm          |
    |1.9 cm          |
    +----------------+
    only showing top 20 rows
    
    


```python
@udf(returnType=ArrayType(StringType()))
def only_dimensions_in_cm(s):
     return re.findall(r"[-+]?\d*\.\d+|\d+", s)
```


```python
dataframe_4 = dataframe_3.withColumn("dimensions_in_cm_array", only_dimensions_in_cm(col("dimensions_in_cm")))

```


```python
dataframe_4.printSchema()
dataframe_4.select("dimensions_in_cm_array").show(20, truncate=False)
```

    root
     |-- Object ID: string (nullable = true)
     |-- Dimensions: string (nullable = true)
     |-- dimensions_in_cm: string (nullable = true)
     |-- dimensions_in_cm_array: array (nullable = true)
     |    |-- element: string (containsNull = true)
    
    +----------------------+
    |dimensions_in_cm_array|
    +----------------------+
    |[1.7]                 |
    |[1.7]                 |
    |[1.7]                 |
    |[1.7]                 |
    |[1.7]                 |
    |[1.7]                 |
    |[1.7]                 |
    |[1.3]                 |
    |[2.9]                 |
    |[2.9]                 |
    |[2.9]                 |
    |[2.9]                 |
    |[2.9]                 |
    |[2.9]                 |
    |[1.3]                 |
    |[1.3]                 |
    |[1.9]                 |
    |[1.9]                 |
    |[1.9]                 |
    |[1.9]                 |
    +----------------------+
    only showing top 20 rows
    
    


```python
s=['101.6', '64.8', '87.6']
[int(i) if i.isdigit() else float(i) for i in s]
```




    [101.6, 64.8, 87.6]




```python
@udf(returnType=ArrayType(FloatType()))
def only_dimensions_in_cm(s):
     return [float(i) for i in s]
```


```python
dataframe_5 =dataframe_4.withColumn("dimensions_in_cm_array_float", only_dimensions_in_cm(col("dimensions_in_cm_array")))
```


```python
@udf(returnType=FloatType())
def max_dimensions(s):
     if s:
        return max(s)
```


```python
dataframe_6 = dataframe_5.withColumn("max_dimension", max_dimensions(col("dimensions_in_cm_array_float")))
```


```python
dataframe_6.select('Object ID','dimensions_in_cm_array_float','max_dimension').show(40)
```

    +---------+----------------------------+-------------+
    |Object ID|dimensions_in_cm_array_float|max_dimension|
    +---------+----------------------------+-------------+
    |        3|                       [1.7]|          1.7|
    |        4|                       [1.7]|          1.7|
    |        5|                       [1.7]|          1.7|
    |        6|                       [1.7]|          1.7|
    |        7|                       [1.7]|          1.7|
    |        8|                       [1.7]|          1.7|
    |        9|                       [1.7]|          1.7|
    |       15|                       [1.3]|          1.3|
    |       16|                       [2.9]|          2.9|
    |       17|                       [2.9]|          2.9|
    |       18|                       [2.9]|          2.9|
    |       19|                       [2.9]|          2.9|
    |       20|                       [2.9]|          2.9|
    |       21|                       [2.9]|          2.9|
    |       22|                       [1.3]|          1.3|
    |       23|                       [1.3]|          1.3|
    |       24|                       [1.9]|          1.9|
    |       25|                       [1.9]|          1.9|
    |       26|                       [1.9]|          1.9|
    |       27|                       [1.9]|          1.9|
    |       28|                       [1.9]|          1.9|
    |       29|                       [1.9]|          1.9|
    |       30|                       [1.9]|          1.9|
    |       31|                       [1.9]|          1.9|
    |       32|                       [5.4]|          5.4|
    |       33|             [7.0, 8.9, 7.0]|          8.9|
    |       34|          [61.9, 37.1, 13.0]|         61.9|
    |       35|          [49.4, 33.0, 23.5]|         49.4|
    |       36|         [101.6, 64.8, 87.6]|        101.6|
    |       37|                      [30.5]|         30.5|
    |       38|                      [31.4]|         31.4|
    |       39|                [27.9, 22.9]|         27.9|
    |       40|                      [16.7]|         16.7|
    |       41|                      [15.9]|         15.9|
    |       42|          [78.4, 63.5, 35.2]|         78.4|
    |       43|          [78.4, 63.5, 35.2]|         78.4|
    |       44|                      [68.6]|         68.6|
    |       45|                      [68.6]|         68.6|
    |       46|                      [40.6]|         40.6|
    |       47|                      [40.6]|         40.6|
    +---------+----------------------------+-------------+
    only showing top 40 rows
    
    


```python
dataframe_7= dataframe_6.select(col("Object ID").alias("object_id"), col("max_dimension"))
```


```python
dataframe_7.show(40)
```

    +---------+-------------+
    |object_id|max_dimension|
    +---------+-------------+
    |        3|          1.7|
    |        4|          1.7|
    |        5|          1.7|
    |        6|          1.7|
    |        7|          1.7|
    |        8|          1.7|
    |        9|          1.7|
    |       15|          1.3|
    |       16|          2.9|
    |       17|          2.9|
    |       18|          2.9|
    |       19|          2.9|
    |       20|          2.9|
    |       21|          2.9|
    |       22|          1.3|
    |       23|          1.3|
    |       24|          1.9|
    |       25|          1.9|
    |       26|          1.9|
    |       27|          1.9|
    |       28|          1.9|
    |       29|          1.9|
    |       30|          1.9|
    |       31|          1.9|
    |       32|          5.4|
    |       33|          8.9|
    |       34|         61.9|
    |       35|         49.4|
    |       36|        101.6|
    |       37|         30.5|
    |       38|         31.4|
    |       39|         27.9|
    |       40|         16.7|
    |       41|         15.9|
    |       42|         78.4|
    |       43|         78.4|
    |       44|         68.6|
    |       45|         68.6|
    |       46|         40.6|
    |       47|         40.6|
    +---------+-------------+
    only showing top 40 rows
    
    


```python
dataframe_7.write.parquet("met6.parquet")
```


```python
def does_it_fit(object_id, dimensions):
        parDF1=spark1.read.parquet("met7.parquet")
        parDF2 = parDF1.filter((parDF1.object_id == object_id))
        if parDF2.rdd.isEmpty():
            print("Cant say for sure. Please check the catalog for more details")
        else:
            parDF3 = parDF2.filter(parDF2.max_dimension < dimensions)
            if parDF3.rdd.isEmpty():
                print("It will not fit")
            else:
                print("It will fit")
```


```python
does_it_fit(14,.45)
```


```python
does_it_fit(33,45)
```


```python
does_it_fit(39,27)
```
