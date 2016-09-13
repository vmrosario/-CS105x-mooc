#import sparkcontext and sparkconf
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("test")
sc = SparkContext(conf=conf)

# Display the type of the Spark sqlContext
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# Display the type of the Spark sqlContext
type(sqlContext)


# List sqlContext's attributes
dir(sqlContext)

# Use help to obtain more detailed information
help(sqlContext)

# After reading the help we've decided we want to use sc.version to see what version of Spark we are running
sc.version

# Help can be used on any Python object
help(map)

# MAGIC We will use a third-party Python testing library called [fake-factory](https://pypi.python.org/pypi/fake-factory/0.5.3) to create a collection of fake person records.
from faker import Factory
fake = Factory.create()
fake.seed(4321)
fake.name

#We're going to use this factory to create a collection of randomly generated people records.
# Each entry consists of last_name, first_name, ssn, job, and age (at least 1)
from pyspark.sql import Row
def fake_entry():
  name = fake.name().split()
  return (name[1], name[0], fake.ssn(), fake.job(), abs(2016 - fake.date_time().year) + 1)

# Create a helper function to call a function repeatedly
def repeat(times, func, *args, **kwargs):
    for _ in xrange(times):
        yield func(*args, **kwargs)

data = list(repeat(10000, fake_entry))

#`data` is just a normal Python list, containing Python tuples objects. Let's look at the first item in the list:
data[0]

#We can check the size of the list using the Python `len()` function.
len(data)

#To create the DataFrame, we'll use `sqlContext.createDataFrame()`, and we'll pass our array of data in as an argument to that function. Spark will create a new set of input data based on data that is passed in.  A DataFrame requires a _schema_, which is a list of columns, where each column has a name and a type. Our list of data has elements with types (mostly strings, but one integer). We'll supply the rest of the schema and the column names as the second argument to `createDataFrame()`.
#Let's view the help for `createDataFrame()`.
help(sqlContext.createDataFrame)

dataDF = sqlContext.createDataFrame(data, ('last_name', 'first_name', 'ssn', 'occupation', 'age'))

#Let's see what type `sqlContext.createDataFrame()` returned.
print 'type of dataDF: {0}'.format(type(dataDF))

#Let's take a look at the DataFrame's schema and some of its rows.
dataDF.printSchema()

#We can register the newly created DataFrame as a named table, using the `registerDataFrameAsTable()` method.
sqlContext.registerDataFrameAsTable(dataDF, 'dataframe')

#What methods can we call on this DataFrame?
help(dataDF)

#How many partitions will the DataFrame be split into?
dataDF.rdd.getNumPartitions()

 
#You can examine the query plan using the `explain()` function on a DataFrame. By default, `explain()` only shows you the final physical plan; however, if you pass it an argument of `True`, it will show you all phases.
#Let's add a couple transformations to our DataFrame and look at the query plan on the resulting transformed DataFrame. Don't be too concerned if it looks like gibberish. As you gain more experience with Apache Spark, you'll begin to be able to use `explain()` to help you understand more about your DataFrame operations.
newDF = dataDF.distinct().select('*')
newDF.explain(True)

# Transform dataDF through a select transformation and rename the newly created '(age -1)' column to 'age'
# Because select is a transformation and Spark uses lazy evaluation, no jobs, stages,
# or tasks will be launched when we run this code.
subDF = dataDF.select('last_name', 'first_name', 'ssn', 'occupation', (dataDF.age - 1).alias('age'))

#Let's take a look at the query plan.
subDF.explain(True)

# Let's collect the data
results = subDF.collect()
print results

#A better way to visualize the data is to use the `show()` method. If you don't tell `show()` how many rows to display, it displays 20 rows.
subDF.show()

#If you'd prefer that `show()` not truncate the data, you can tell it not to:
subDF.show(n=30, truncate=False)

#In Databricks, there's an even nicer way to look at the values in a DataFrame: The `display()` helper function.
display(subDF)
 
#Each task counts the entries in its partition and sends the result to your SparkContext, which adds up all of the counts. The figure on the right shows what would happen if we ran `count()` on a small example dataset with just four partitions.
print dataDF.count()
print subDF.count()

#To view the filtered list of elements less than 10, we need to create a new list on the driver from the distributed data on the executor nodes.  We use the `collect()` method to return a list that contains all of the elements in this filtered DataFrame to the driver program.
filteredDF = subDF.filter(subDF.age < 10)
filteredDF.show(truncate=False)
filteredDF.count()

#Here, instead of defining a separate function for the `filter()` transformation, we will use an inline `lambda()` function and we will register that lambda as a Spark _User Defined Function_ (UDF). A UDF is a special wrapper around a function, allowing the function to be used in a DataFrame query.
from pyspark.sql.types import BooleanType
less_ten = udf(lambda s: s < 10, BooleanType())
lambdaDF = subDF.filter(less_ten(subDF.age))
lambdaDF.show()
lambdaDF.count()

# Let's collect the even values less than 10
even = udf(lambda s: s % 2 == 0, BooleanType())
evenDF = lambdaDF.filter(even(lambdaDF.age))
evenDF.show()
evenDF.count()
 
#Instead of using the `collect()` action, we can use the `take(n)` action to return the first _n_ elements of the DataFrame. The `first()` action returns the first element of a DataFrame, and is equivalent to `take(1)[0]`.
print "first: {0}\n".format(filteredDF.first())

print "Four of them: {0}\n".format(filteredDF.take(4))

#This looks better:
display(filteredDF.take(4))


#dataDF.orderBy(dataDF['age'])  # sort by age in ascending order; returns a new DataFrame
#dataDF.orderBy(dataDF.last_name.desc()) # sort by last name in descending order
# Get the five oldest people in the list. To do that, sort by age in descending order.
display(dataDF.orderBy(dataDF.age.desc()).take(5))

display(dataDF.orderBy('age').take(5))

#`distinct()` filters out duplicate rows, and it considers all columns. Since our data is completely randomly generated (by `fake-factory`), it's extremely unlikely that there are any duplicate rows:
print dataDF.count()
print dataDF.distinct().count()

#To demonstrate `distinct()`, let's create a quick throwaway dataset.
tempDF = sqlContext.createDataFrame([("Joe", 1), ("Joe", 1), ("Anna", 15), ("Anna", 12), ("Ravi", 5)], ('name', 'score'))
tempDF.show()
tempDF.distinct().show()

#`dropDuplicates()` is like `distinct()`, except that it allows us to specify the columns to compare. For instance, we can use it to drop all rows where the first name and last name duplicates (ignoring the occupation and age columns).
print dataDF.count()
print dataDF.dropDuplicates(['first_name', 'last_name']).count()

#`drop()` is like the opposite of `select()`: Instead of selecting specific columns from a DataFrame, it drops a specifed column from a DataFrame.
#Here's a simple use case: Suppose you're reading from a 1,000-column CSV file, and you have to get rid of five of the columns. Instead of selecting 995 of the columns, it's easier just to drop the five you don't want.
dataDF.drop('occupation').drop('age').show()

#`groupBy()` is one of the most powerful transformations. It allows you to perform aggregations on a DataFrame.
#The most commonly used aggregation function is count(),
#but there are others (like sum(), max(), avg().
#These aggregation functions typically create a new column and return a new DataFrame.
dataDF.groupBy('occupation').count().show(truncate=False)
dataDF.groupBy().avg('age').show(truncate=False)

#We can also use `groupBy()` to do aother useful aggregations:
print "Maximum age: {0}".format(dataDF.groupBy().max('age').first()[0])
print "Minimum age: {0}".format(dataDF.groupBy().min('age').first()[0])

#When analyzing data, the `sample()` transformation is often quite useful. It returns a new DataFrame with a random sample of elements from the dataset.  It takes in a `withReplacement` argument, which specifies whether it is okay to randomly pick the same item multiple times from the parent DataFrame (so when `withReplacement=True`, you can get the same item back multiple times). It takes in a `fraction` parameter, which specifies the fraction elements in the dataset you want to return. (So a `fraction` value of `0.20` returns 20% of the elements in the DataFrame.) It also takes an optional `seed` parameter that allows you to specify a seed value for the random number generator, so that reproducible results can be obtained.
sampledDF = dataDF.sample(withReplacement=False, fraction=0.10)
print sampledDF.count()
sampledDF.show()

print dataDF.sample(withReplacement=False, fraction=0.05).count()

# Cache the DataFrame
filteredDF.cache()
# Trigger an action
print filteredDF.count()
# Check if it is cached
print filteredDF.is_cached

# If we are done with the DataFrame we can unpersist it so that its memory can be reclaimed
filteredDF.unpersist()
# Check if it is cached
print filteredDF.is_cached


# Cleaner code through lambda use
myUDF = udf(lambda v: v < 10)
subDF.filter(myUDF(subDF.age) == True)

#To make the expert coding style more readable, enclose the statement in parentheses and put each method, transformation, or action on a separate line.
# Final version
from pyspark.sql.functions import *
(dataDF
 .filter(dataDF.age > 20)
 .select(concat(dataDF.first_name, lit(' '), dataDF.last_name), dataDF.occupation)
 .show(truncate=False)
 )

