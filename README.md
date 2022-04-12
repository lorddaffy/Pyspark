# Pyspark
**Practicing on PySpark, Manipulating data, Machine learning pipelines and  Model tuning and selection**

__________________________________________________

> Creating a SparkSession
- Import SparkSession from pyspark.sql.
- Make a new SparkSession called spark using SparkSession.builder.getOrCreate().
- Print spark to the console to verify it's a SparkSession.
- See what tables are in your cluster by calling spark.catalog.listTables() and printing the result!
```
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create spark
spark = SparkSession.builder.getOrCreate()

# Print spark
print(spark)

# See what tables are in your cluster
print(spark.catalog.listTables())
```
__________________________________________________

> Quering
- Import SparkSession from pyspark.sql.
- Make a new SparkSession called spark using SparkSession.builder.getOrCreate().
- See what tables are in your cluster by calling spark.catalog.listTables() and printing the result!
- get the first 10 rows of the flights table and save the result to a variable. 
- Use the DataFrame method to print query.
```
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark
spark = SparkSession.builder.getOrCreate()

# See what tables are in your cluster

print(spark.catalog.listTables())

query = "FROM flights SELECT * LIMIT 10"

# Get the first 10 rows of flights
flights10 = spark.sql(query)

# Show the results
flights10.show()
```
__________________________________________________

> Pandafy
- Import SparkSession from pyspark.sql.
- Make a new SparkSession called spark using SparkSession.builder.getOrCreate().
- Use This Query = `SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest`
- create a pandas DataFrame.
- Print the .head() of DataFrame to the console.
``` 
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark
spark = SparkSession.builder.getOrCreate()

query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"

# Run the query
flight_counts = spark.sql(query)

# Convert the results to a pandas DataFrame
pd_counts = flight_counts.toPandas()

# Print the head of pd_counts
print(pd_counts.head())
```
__________________________________________________

- Create DataFrame of any 10 rows
- Move Data from the DataFrame Pandas to Spark
- Examine the tables
- Add the Data to the catalog
- Examine the tables in the catalog
``` 
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession
import pandas as pd

# Create pd_temp
pd_temp = pd.DataFrame(np.random.random(10))

# Create spark_temp from pd_temp
spark_temp = spark.createDataFrame(pd_temp)

# Examine the tables in the catalog
print(spark.catalog.listTables())

# Add spark_temp to the catalog
spark_temp.createOrReplaceTempView("temp")

# Examine the tables in the catalog again
print(spark.catalog.listTables())
```
__________________________________________________

> Dropping the middle man

- Make a new SparkSession called spark using SparkSession.builder.getOrCreate().
- The File Path = "/usr/local/share/datasets/airports.csv"
- Create a Spark DataFrame called airports
- Take the column names from the first line of the file.
- Print out this DataFram.
``` 
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

file_path = "/usr/local/share/datasets/airports.csv"

# Create my_spark
spark = SparkSession.builder.getOrCreate()

# Read in the airports data
airports = spark.read.csv(file_path, header=True)

# Show the data
airports.show()
```
__________________________________________________

### Manipulating data

> Updating a Spark DataFrame is somewhat different than working in pandas because the Spark DataFrame is immutable. This means that it can't be changed, and so columns can't be updated in place; To overwrite the original DataFrame you must reassign the returned DataFrame using the method like so:
>>`df = df.withColumn("newCol", df.oldCol + 1)`

- Make a new SparkSession called spark using SparkSession.builder.getOrCreate().
- Create a DataFrame containing the values of the flights table in the .catalog. Save it as flights
- Show the head of flights
- Check the output: the column air_time contains the duration of the flight in minutes.
- Update flights to include a new column called duration_hrs, that contains the duration of each flight in hours (you'll need to divide air_time by the number of minutes in an hour).
``` 
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create the DataFrame flights
flights = spark.table("flights")

# Show the head
flights.show()

# Add duration_hrs
flights = flights.withColumn("duration_hrs",(flights.air_time/60))
#flights.show()

```
__________________________________________________
