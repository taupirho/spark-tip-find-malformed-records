Often when you’re reading in text files with a user specified schema definition you’ll find that not all the records in the file 
will meet that definition. In other words there will be malformed records present. Either fields will be missing, there will be 
extra unaccounted for fields or some fields will contain data that’s not of the type specified in the schema. For small files 
tracking down these records isn’t a problem but for the huge mega files of the big data world these can be a real headache to 
sort out. The question is, what can we do in these scenarios?

For the purposes of this article assume we are processing a text data file that contains information about the most populous 
cities in the world. The file contains the city name, the country it’s in, its latitude and longitude coordinates and finally 
its population as was in 2015. Below is an example of such a file containing a few common malformed records you might 
typically encounter. The code below was run using a Jupyter notebook server 5.6, python 3.6.5 and Spark 2.3.1 .

```
City, Country, Latitude,Longitude,Population

Tokyo,Japan,35.6895,139.69171,38001000

Delhi,India,28.66667,77.21667,25703000

Shanghai,China,31.22,121.46,23741000,ABCDE

São Paulo,Brazil,-23.55

Mumbai,India, India,72.880838,21043000
```

We manually define its schema as follows:

```
from pyspark.sql.types import StructField, StructType, DoubleType,StringType,LongType

 

citySchema = StructType([

        StructField("city", StringType(), True),

        StructField("country", StringType(), True),

        StructField("latitude", DoubleType(), True),

        StructField("longitude", DoubleType(), True),

        StructField("population", LongType(), True),

            ])
```

Records 1 and 2 are valid, record 3 has a spurious extra text field at its end. Record 4 has missing fields and record 
5 has repeated the string India where the latitude value should be.

To help dealing with bad or missing data, Spark is able to use three different parse modes when reading in data. These are:

#Permissive

This is the default behaviour and tells Spark to insert nulls into fields that could not be properly parsed. Use this mode 
when you want to read in as much of the data as possible and deal with invalid data at a later stage. Let’s see what we get 
when we use this mode on our data set.

```
df = spark.read.format("csv") \
    .option("header", "true").schema("citySchema") \
    .option("delimiter", ',').option("mode","permissive")
    .load("file:///d:/tmp/cities.txt")

 
df.show()


+---------+-------+--------+---------+----------+
|     city|country|latitude|longitude|population|
+---------+-------+--------+---------+----------+
|    Tokyo|  Japan| 35.6895|139.69171|  38001000|
|    Delhi|  India|28.66667| 77.21667|  25703000|
| Shanghai|  China|   31.22|   121.46|  23741000|
|Sao Paulo| Brazil|  -23.55|     null|      null|
|     null|   null|    null|     null|      null|
+---------+-------+--------+---------+----------+
```
We can see that the extra field in line 3 is ignored, the
missing values from row 4 are nullified and also that the last row of the dataset, since we encountered a string where a 
double was expected, is treated as being corrupt therefore all fields are set to a null value. That missing value in record 
three is an issue. Not that Spark ignored it – that bit is OK, but it would have been nice to have been informed that something
was not quit right with it. If you are a Scala programmer there is a way you can get this information. Check the end of this 
article to see how.

#Dropmalformed

In this mode Spark will drop any records where one or more fields could not be parsed correctly. You would use this mode when 
you only want to read in data that exactly meets your schema definition.

```
df = spark.read.format("csv") \
    .option("header", "true").schema("citySchema") \
    .option("delimiter", ',').option("mode","dropmalformed")\
    .load("file:///d:/tmp/cities.txt")

 
 

df.show()

 

+-----+-------+--------+---------+----------+
| city|country|latitude|longitude|population|
+-----+-------+--------+---------+----------+
|Tokyo|  Japan| 35.6895|139.69171|  38001000|
|Delhi|  India|28.66667| 77.21667|  25703000|
+-----+-------+--------+---------+----------+
```

#Failfast

In this mode Spark will simply return an error if there is any data at all that it can’t parse properly and no data will be 
loaded. Use this mode when errors in the input data cannot be tolerated.

```
df = spark.read.format("csv") \

    .option("header", "true").schema("citySchema") \

    .option("delimiter", ',').option("mode","failfast").load("file:///d:/tmp/cities.txt")


df.show()

 

Py4JJavaError                             Traceback (most recent call last)

<ipython-input-27-f0ce66b782a1> in <module>()

     10 df = spark.read.format("csv")     .option("header", "true").schema(citySchema)     .option("delimiter", ',').option("mode","FAILFAST").load("file:///d:/tmp/cities.txt")

     11

---> 12 df.show()

etc …

etc….
````

We saw earlier that in the default Permissive mode the extra data field in record three of our input was ignored – as it 
should have been – but there was no record of it having been ignored. Depending on your requirements this may or may not 
be an issue. However if you are a user of Spark/Scala there is way to catch this by adding the built-in _corrupt_record 
directive to your schema definition. You just add it on to the end of your regular schema definition and if any of the data 
is missing, or malformed in any way, the whole input record is assigned to this dummy column name. It’s best to illustrate 
this with some example code. This was run in a spark/scala shell running spark 2.3.1 and scala 2.11.8

```
import org.apache.spark.sql.types._

val schema = new StructType()

.add("city",StringType,true)

.add("country",StringType,true)

.add("latitude",DoubleType,true)

.add("longitude",DoubleType,true)

.add("population",LongType,true)

.add("_corrupt_record", StringType, true)
```

Now see what happens when we read in our original cities data file.

```
val df = spark.read.format("csv")

.option("header", "true")

.schema(schema)

.load("file:///d:/tmp/cities.txt")

 
df.show()

 
+---------+-------+--------+---------+----------+--------------------+
|     city|country|latitude|longitude|population|     _corrupt_record|
+---------+-------+--------+---------+----------+--------------------+
|    Tokyo|  Japan| 35.6895|139.69171|  38001000|                null|
|    Delhi|  India|28.66667| 77.21667|  25703000|                null|
| Shanghai|  China|   31.22|   121.46|  23741000|Shanghai,China, 3...|
|Sao Paulo| Brazil|  -23.55|     null|      null|Sao Paulo,Brazil,...|
|     null|   null|    null|     null|      null|Mumbai,India,Indi...|
+---------+-------+--------+---------+----------+--------------------+
```

As you can see an extra column is shown, the _corrupt_record. This column is set to null if there are no issues with 
the incoming data and set to the value of the whole record if there is an issue. This makes it very easy to screen 
out invalid data.

