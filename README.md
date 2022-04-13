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

> Filtering Data

- Use the .filter() method to find all the flights that flew over 1000 miles the two ways
- Print heads of both DataFrames and make sure they're actually equal!
``` 
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Filter flights by passing a string
long_flights1 = flights.filter("distance > 1000")

# Filter flights by passing a column of boolean values
long_flights2 = flights.filter(flights.distance > 1000)

# Print the data to check they're equal
long_flights1.show()
long_flights2.show()
```
__________________________________________________

> Selecting & Filtering

- Select the columns "tailnum", "origin", and "dest" from flights by passing the column names as strings.
- Select the columns "origin", "dest", and "carrier" using the df.colName syntax and then filter the result to only keep flights from SEA to PDX.
``` 
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Select the first set of columns
selected1 = flights.select("tailnum", "origin", "dest")

# Select the second set of columns
temp = flights.select(flights.origin, flights.dest, flights.carrier)

# Define first filter
filterA = flights.origin == "SEA"

# Define second filter
filterB = flights.dest == "PDX"

# Filter the data, first by filterA then by filterB
selected2 = temp.filter(filterA).filter(filterB)
```
__________________________________________________

> Create a table of the average speed of each flight both ways.

- Calculate average speed by dividing the distance by the air_time (converted to hours). Use the .alias() method name this column "avg_speed".
- Select the columns "origin", "dest", "tailnum", and avg_speed
- Create the same table using .selectExpr().
``` 
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Define avg_speed
avg_speed = (flights.distance/(flights.air_time/60)).alias("avg_speed")

# Select the correct columns
speed1 = flights.select("origin", "dest", "tailnum", avg_speed)

# Create the same table using a SQL expression
speed2 = flights.selectExpr("origin", "dest", "tailnum", "distance/(air_time/60) as avg_speed")
```
__________________________________________________

- Find the length of the shortest (in terms of distance) flight that left PDX.
- Find the length of the longest (in terms of time) flight that left SEA.
``` 
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Find the shortest flight from PDX in terms of distance
flights.filter(flights.origin == "PDX").groupBy().min("distance").show()

# Find the longest flight from SEA in terms of air time
flights.filter(flights.origin == "SEA").groupBy().max("air_time").show()
```
__________________________________________________
- Get the average air time of Delta Airlines flights (where the carrier column has the value "DL") that left SEA. The place of departure is stored in the column origin and print the result.
- Get the total number of hours all planes in this dataset spent in the air by creating a column called duration_hrs from the column air_time and print the result.
``` 
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Average duration of Delta flights
flights.filter(flights.carrier == "DL").filter(flights.origin == "SEA").groupBy().avg("air_time").show()

# Total hours in the air
flights.withColumn("duration_hrs", flights.air_time/60).groupBy().sum("duration_hrs").show()
```
__________________________________________________
> Grouping and Aggregating I

- Create a DataFrame that is grouped by the column tailnum and count the number of flights each plane made.
- Create a DataFrame that is grouped by the column origin, find average duration of flights from PDX and SEA.

``` 
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Group by tailnum
flights.groupBy("tailnum").count().show()
# Group by origin
flights.groupBy("origin").avg("air_time").show()
```
__________________________________________________

> Grouping and Aggregating II
- Import the submodule pyspark.sql.functions.
- Create a GroupedData table that's grouped by both the month and dest columns. Refer to the two columns by passing both strings as separate arguments.
- Get the average dep_delay in each month for each destination.
- Find the standard deviation of dep_delay.
``` 
# Import pyspark.sql.functions as F
import pyspark.sql.functions as F

# Group by month and dest
# Average departure delay by month and destination
flights.groupBy("month","dest").avg("dep_delay").show()

# Group by month and dest
# Standard deviation of departure delay
flights.groupBy("month","dest").agg(F.stddev("dep_delay")).show()
```
__________________________________________________

> Joining
- Examine the airports DataFrame
- Rename the faa column in airports to dest
- Join the flights with the airports DataFrame on the dest column by calling the .join() method on flights
- Examine the data after joining.
``` 
# Import pyspark.sql.functions as F
import pyspark.sql.functions as F
# Examine the data
airports.show()

# Rename the faa column
airports = airports.withColumnRenamed("faa", "dest")

# Join the DataFrames
flights_with_airports = flights.join(airports, on="dest", how="leftouter")

# Examine the new DataFrame
flights_with_airports.show()
```
__________________________________________________
