# Databricks notebook source
sc #SparkContext is the primary point of entry for Spark capabilities

# COMMAND ----------

spark #Three entry points: SparkContext, SQLContext, and HiveContext. SparkSession combines the functionality of all those contexts. 

# COMMAND ----------

# MAGIC %md
# MAGIC #Counting word frequencies example

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Read txt file from Databricks File System

# COMMAND ----------

rdd = sc.textFile("dbfs:/FileStore/data.txt") #transformation - lazy operation
rdd = rdd.take(10) #actions
rdd

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Create an RDD from a String

# COMMAND ----------

data = 'test1 test2 test3 test2 test4 test5 test6 test2 test7'
rdd = sc.parallelize([data]) #transformation
results = rdd.collect() #actions
results

# COMMAND ----------

words = rdd.flatMap(lambda x: x.split(' ')) #transformation - flatMap() transformation flattens the RDD after applying the function and returns a new RDD
words.collect()

# COMMAND ----------

words = words.filter(lambda x: x.endswith("1") == False) #transformation - filters out the data that ends with 1
words.collect()

# COMMAND ----------

pairs = words.map(lambda x: (x,1)) #transformation -we are adding a new column with a value of 1 for each word to create key value pairs -  map() transformation is used the apply any complex operations like adding a column, updating a column e.t.c, the output of map transformations would always have the same number of records as the input.
pairs.collect()

# COMMAND ----------

data = pairs.reduceByKey(lambda x,y: x+y) #transformation - reduceByKey() merges the values for each key with the function specified. In our case the sum function.
data.collect()

# COMMAND ----------

outcome = data.map(lambda x: (x[1],x[0])).sortByKey(False) #transformation,transformation,action - sortByKey() transformation is used to sort RDD elements on key. 
outcome

# COMMAND ----------

outcome.saveAsTextFile("dbfs:/FileStore/resultTest.txt") #action

# COMMAND ----------

# MAGIC %md
# MAGIC ###A few other things to keep in mind

# COMMAND ----------

# MAGIC %md
# MAGIC The **repartition** method makes new partitions and evenly distributes the data in the new partitions (performs a **full shuffle**). With repartition you can increase or decrease the number of partitions

# COMMAND ----------

print(data.getNumPartitions()) 
data.glom().collect()

# COMMAND ----------

data = data.repartition(3)
print(data.getNumPartitions()) 

# COMMAND ----------

data.glom().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC With **coalesce** you can only decrease the number of partitions. Coalesce uses existing partitions to **minimize data shuffling**. However, coalesce results in partitions with much different size usually while repartition results in roughly equal sized partitions.

# COMMAND ----------

data = data.coalesce(2)
data.glom().collect() #PySpark RDD's glom() method returns a RDD holding the content of each partition

# COMMAND ----------

# MAGIC %md
# MAGIC **Caching or persistence** are optimization techniques for iterative Spark computations. It helps to save intermediate results so we can reuse them in subsequent stages. These intermediate results as RDDs are thus kept in memory (default) or more solid storages like disk.

# COMMAND ----------

data.cache()
data.persist() #you can define the storage level
