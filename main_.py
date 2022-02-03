#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StringType, ArrayType, DoubleType
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as func
import re
from pyspark import SparkContext


# In[2]:


SparkContext.setSystemProperty('spark.executor.memory', '4g')
sc = SparkContext("local","spark")
spark1 = SparkSession.builder.appName('spark').getOrCreate()
data_frame = spark1.read.csv('MetObjects.csv',inferSchema=True,header=True)


# In[3]:


dataframe_1= data_frame.select("Object ID","Dimensions")
type(dataframe_1)


# In[4]:


dataframe_1.sample(withReplacement=False, fraction=0.01).show(truncate=False)


# In[5]:


dataframe_2=dataframe_1.filter(dataframe_1.Dimensions.contains('cm'))
type(dataframe_2)


# In[6]:


@udf(returnType=StringType())
def dimensions_in_cm(s):
     q=s[s.find("(")+1:s.find(")")]
     return q


# In[7]:


type(dataframe_2["Dimensions"][1])


# In[8]:


dataframe_3 = dataframe_2.withColumn("dimensions_in_cm", dimensions_in_cm(col("Dimensions")))
dataframe_3.select("dimensions_in_cm").show(20, truncate=False)


# In[9]:


@udf(returnType=ArrayType(StringType()))
def only_dimensions_in_cm(s):
     return re.findall(r"[-+]?\d*\.\d+|\d+", s)


# In[10]:


dataframe_4 = dataframe_3.withColumn("dimensions_in_cm_array", only_dimensions_in_cm(col("dimensions_in_cm")))


# In[11]:


dataframe_4.printSchema()
dataframe_4.select("dimensions_in_cm_array").show(20, truncate=False)


# In[12]:


s=['101.6', '64.8', '87.6']
[int(i) if i.isdigit() else float(i) for i in s]


# In[13]:


@udf(returnType=ArrayType(FloatType()))
def only_dimensions_in_cm(s):
     return [float(i) for i in s]


# In[14]:


dataframe_5 =dataframe_4.withColumn("dimensions_in_cm_array_float", only_dimensions_in_cm(col("dimensions_in_cm_array")))


# In[15]:


@udf(returnType=FloatType())
def max_dimensions(s):
     if s:
        return max(s)


# In[16]:


dataframe_6 = dataframe_5.withColumn("max_dimension", max_dimensions(col("dimensions_in_cm_array_float")))


# In[17]:


dataframe_6.select('Object ID','dimensions_in_cm_array_float','max_dimension').show(40)


# In[18]:


dataframe_7= dataframe_6.select(col("Object ID").alias("object_id"), col("max_dimension"))


# In[19]:


dataframe_7.show(40)


# In[ ]:


dataframe_7.write.parquet("met6.parquet")


# In[ ]:


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


# In[ ]:


does_it_fit(14,.45)


# In[ ]:


does_it_fit(33,45)


# In[ ]:


does_it_fit(39,27)

