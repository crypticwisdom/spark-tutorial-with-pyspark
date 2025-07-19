# from pyspark.sql import SparkSession
# spark =SparkSession.builder.appName("Spark with Jupyter").getOrCreate()

# df = spark.read.option('header', 'true').option('inferSchema', 'true').option('sep', '\t').csv('data/testv.csv')  # Read the CSV file with header, infer schema, and specify tab as the separator
# df.show(10, truncate=False)  # Show the first 10 rows of the DataFrame without truncating the output

# df.printSchema()
# print(df.schema)
# print(df.dtypes)  # Get the data types of each column in the DataFrame as a list of tuples (column_name, data_type)

# print(df.columns)
# # df.rows  # Get all the rows in the DataFrame as a list of Row objects
# df.head(10)
# df.tail(10)

# from pyspark.sql.functions import col, concat, lit
# df.select(col('MSISDN')).show()

# df.select('MSISDN').show()  # Select the 'MSISDN' column and show its values
# df.select('MSISDN', 'NID').show()  # Select multiple columns and
# df.select(['MSISDN', 'NID']).show()  # Select multiple columns using a list
# # df.MSISDN * 3  # Select the 'MSISDN' column using dot notation

# # select, add new col, rename col, 
# phone_number_df = df.select('MSISDN', 'NID', 'EMAIL') \
#                     .withColumn('PHONE_NUMBER', concat(lit('230'), col('MSISDN'))) \
#                     .withColumnRenamed('NID', 'NATIONAL_ID')
# phone_number_df.show(5, truncate=False)  # Show the first 10 rows of the DataFrame with the new 'PHONE_NUMBER' column



from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col

spark = SparkSession.builder.master('local[*]').appName('Test2').getOrCreate()
spark

df = spark.read.option('header', 'true').option('inferSchema', 'true').option('sep', '\t').csv('data/testv.csv')
# df.show(5)

# df2 = spark.read.csv('data/testv.csv', header=True, inferSchema=True, sep='\t')
# df2.show(5)

df.printSchema()
df.columns

new_df = df.select(
    col('MSISDN'), 
    col('NID'), 
    col('POSTPAID_MAINPRODUCT'), 
    col('EMAIL')
).withColumn(
    'PHONE_NUMBER',
    concat(lit('230'), col('MSISDN'))
).withColumnRenamed(
    'NID', 'NATIONAL_ID'
)

nas = new_df.na.drop(how='all', thresh=4)
nas.show()
