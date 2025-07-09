from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, sum, avg, count, min, max, udf


spark = SparkSession.builder.appName("DataFrame").getOrCreate()

# Create a DataFrame from a csv file
df = spark.read.option("header", True).csv('data/csvFiles/StudentData.csv')
df.show()

# Schema of the DataFrame
df = spark.read.options(inferSchema='True', header='True').csv('data/csvFiles/StudentData.csv')
print("Schema of the DataFrame:")
df.printSchema()

# Create a Schema manually
schema = StructType([
    StructField('age', IntegerType(), True),
    StructField('gender', StringType(), True),
    StructField('name', StringType(), True),
    StructField('course', StringType(), True),
    StructField('roll', StringType(), True),
    StructField('marks', IntegerType(), True),
    StructField('email', StringType(), True),
])

# Create a DataFrame with the schema
df_with_schema = spark.read.options(header='True').schema(schema).csv('data/csvFiles/StudentData.csv')
print("DataFrame with manually defined schema:")
df_with_schema.printSchema()


# Create a DataFrame from a RDD
from pyspark import SparkContext
sc = SparkContext.getOrCreate()
rdd = sc.textFile('data/csvFiles/StudentData.csv')

rdd = rdd.map(lambda x: x.split(','))
header = rdd.first()  # Get the header
rdd = rdd.filter(lambda line: line != header)  # Remove the header from the RDD
rdd = rdd.map(lambda x: (int(x[0]), x[1], x[2], x[3], x[4], int(x[5]), x[6]))  # Convert to tuple with correct types

columns = header
df_from_rdd = spark.createDataFrame(rdd, schema=schema)
df_from_rdd.show()
print("DataFrame created from RDD:")
df_from_rdd.printSchema()

# Select specific columns
df_selected = df.select("name", col('course'), df.marks)
print("Selected columns (name, course, marks):")
df_selected.show()
print("Selected full columns:")
df_selected_full = df.select("*")
df_selected_full.show()

# Show all columns in the DataFrame
print("All columns in the DataFrame:")
print(df.columns)
df.select(df.columns[:2]).show()

# Show the number of rows in the DataFrame
num_rows = df.count()
print("Number of rows in the DataFrame:", num_rows)

# Show first 5 rows of the DataFrame
print("First 5 rows of the DataFrame:")
df.show(5)

# With columns
df = spark.read.options(header='True', inferSchema='True').csv('data/csvFiles/StudentData.csv')

# Change the column types
df_changed_types = df.withColumn("roll", df["roll"].cast(StringType()))
print("DataFrame with changed column types:")
df_changed_types.printSchema()

# Add 10 to each student's marks
df_updated_marks = df.withColumn("marks", df["marks"] + 10)
print("DataFrame with updated marks (10 added to each student's marks):")
df_updated_marks.show()

# Add a new column
df_with_new_column = df.withColumn("passed", df["marks"] >= 50)
print("DataFrame with new column 'passed' (True if marks >= 50):")
df_with_new_column.show()

# Add a new column with a constant value
df_with_constant_column = df.withColumn("constant_value", lit(100))
print("DataFrame with new constant column 'constant_value':")
df_with_constant_column.show()

# Change the column names
df_renamed = df.withColumnRenamed("name", "student_name").withColumnRenamed("marks", "total_marks")
print("DataFrame with renamed columns:")
df_renamed.show()

# Alias for columns
df_alias = df.select(df.name.alias("student_name"), df.marks.alias("total_marks"))
print("DataFrame with aliased columns:")
df_alias.show()

# Filter rows based on a condition
df_filtered = df.filter((df.marks > 50) & (df.course == "DB")) # &, |, ~, etc. can be used for logical operations
print("DataFrame with rows where marks > 50 and course = 'DB':")
df_filtered.show()

courses = ["DB", "MVC", "Cloud"]
# Filter rows based on a list of values
df_filtered_courses = df.filter(df.course.isin(courses))
print("DataFrame with rows where course is in the list [DB, MVC, Cloud]:")
df_filtered_courses.show()

# Filter rows based on a condition with string operations
df_filtered_name = df.filter(df.name.contains("A"))
print("DataFrame with rows where name contains 'A':")
df_filtered_name.show()

# Filter rows where name starts with a specific letter
df_start_with = df.filter(df.name.startswith("D"))
print("DataFrame with rows where name starts with 'D':")
df_start_with.show()

# Filter rows where name ends with a specific letter
df_end_with = df.filter(df.name.endswith("a"))
print("DataFrame with rows where name ends with 'a':")
df_end_with.show()

# like operation
df_like = df.filter(df.name.like("%ss%"))
print("DataFrame with rows where name contain 'aa' using like:")
df_like.show()

# Quiz 1:
df = spark.read.options(header='True', inferSchema='True').csv('data/csvFiles/StudentData.csv')

# Create a new column 'total_marks' let the total marks be 120
df = df.withColumn('total_marks', lit(120))
print("DataFrame with new column 'total_marks' set to 120:")
df.show() 

# Create a new column 'average_marks' cateculate the average marks (marks / total_marks) * 100
df = df.withColumn('average_marks', df.marks / df.total_marks * 100)
print("DataFrame with new column 'average_marks':")
df.show()

# Filter out the students who have average marks greater than 80% in OOP course and save it in new DataFrame
df_oop_high_avg = df.filter((df.course == "OOP") & (df.average_marks > 80))
print("DataFrame with students who have average marks greater than 80% in OOP course:")
df_oop_high_avg.show()

# Filter out the students who have average marks greater than 60% in Cloud course and save it in new DataFrame
df_cloud_high_avg = df.filter((df.course == "Cloud") & (df.average_marks > 60))
print("DataFrame with students who have average marks greater than 60% in Cloud course:")
df_cloud_high_avg.show()

# Print the name and marks of the students from the above DataFrame
print("Name and marks of students from the Cloud course with average marks greater than 60%:")
df_cloud_high_avg.select("name", "marks").show()

# Distinct values in a column
df = spark.read.options(header='True', inferSchema='True').csv('data/csvFiles/StudentData.csv')

distinct_courses = df.select("course").distinct()
print("Distinct courses in the DataFrame:")
distinct_courses.show()

# dropDuplicates
df_duplicates = df.dropDuplicates(["gender", "course"])
print("DataFrame after dropping duplicates based on gender and course:")
df_duplicates.show()

# Quiz 2:
# Write a code to display all the unique rows for age, gender, and course
df_unique_rows = df.select("age", "gender", "course").distinct()
print("Unique rows for age, gender, and course:")
df_unique_rows.show()


# sort the DataFrame by marks in descending order
df_sorted = df.sort(df["marks"].desc())
print("DataFrame sorted by marks in descending order:")
df_sorted.show()

# orderBy
df_ordered = df.orderBy(df["marks"].asc(), df["name"].asc())
print("DataFrame ordered by marks in ascending order:")
df_ordered.show()

# GroupBy 
df_grouped = df.groupBy("course").sum("marks") # sum(), avg(), count(), min(), max() can be used
print("DataFrame grouped by course with sum of marks:")
df_grouped.show()

df_grouped = df.groupBy("course", "gender").count()
print("DataFrame grouped by course and gender with count:")
df_grouped.show()

df_grouped = df.groupBy("course").agg(avg("marks").alias("average_marks"), max("age").alias("max_age"))
print("DataFrame grouped by course with average marks and maximum age:")
df_grouped.show()

# filtering before grouping
df_filtered_grouped = df.filter(df.marks > 50).groupBy("course").count()
print("DataFrame grouped by course with count of students having marks > 50:")
df_filtered_grouped.show()

#  filtering after grouping
df_grouped_filtered = df.groupBy("course").count().where(col("count") > 165)
print("DataFrame grouped by course with count of students > 2:")
df_grouped_filtered.show()

# Quiz 3: 
df = spark.read.options(header='True', inferSchema='True').csv('data/csvFiles/StudentData.csv')

# Display the total numbers students enrolled in each course
df_course_count = df.groupBy("course").count()
print("Total students enrolled in each course:")
df_course_count.show()

# Display the total numbers of male and female students enrolled in each course
df_gender_course_count = df.groupBy("course", "gender").count()
print("Total number of male and female students enrolled in each course:")
df_gender_course_count.show()

# Display the total marks achieved by each gender in each course
df_total_marks_gender_course = df.groupBy("course", "gender").sum("marks")
print("Total marks achieved by each gender in each course:")
df_total_marks_gender_course.show()

# Display the average, minimum, and maximum marks achieved in each course by age group
df_age_grouped = df.groupBy("course", "age").agg(avg("marks").alias("average_marks"),
                                                min("marks").alias("min_marks"),
                                                max("marks").alias("max_marks"))
print("Average, minimum, and maximum marks achieved in each course by age group:")
df_age_grouped.show()

# Quiz 4:

# Word Count
df = spark.read.text("data/WordData.txt")
df_word_count = df.groupBy("value").count()
print("Word count from the text file:")
df_word_count.show()

# UDFs 
df = spark.read.options(header='True', inferSchema='True').csv('data/csvFiles/OfficeData.csv')

def get_total_salary(salary, bonus):
    return salary + bonus

totalSalaryUDF = udf(lambda x, y: get_total_salary(x, y), IntegerType())
df_totalSalary = df.withColumn("total_salary", totalSalaryUDF(df['salary'], df['bonus']))
df_totalSalary.show()


# DF to RDD
df = spark.read.option("header", True).csv('data/csvFiles/StudentData.csv')
print('type of df: ', type(df))

rdd = df.rdd
print('type of rdd: ', type(rdd))

rdd_Male = rdd.filter(lambda x: x[1] == 'Male') # x["gender"]
print(rdd_Male.take(10))

# Spark SQL
df = spark.read.option("header", True).csv('data/csvFiles/StudentData.csv')

df.createOrReplaceTempView("Student")
spark.sql("select course, count(*) from Student group by course").show()

# Write data
df.write.mode("overwrite").options(header="True").csv("data/csvFiles/StudentDataOutput")

print(df.rdd.getNumPartitions())


# Mini Project
df = spark.read.options(header="True", inferSchema="True").csv("data/csvFiles/OfficeDataProject.csv")

# Print the total employees in the company
print("Print the total employees in the company: ",df.count())

# Print the total department in the company
df_department_count = df.select("department").distinct()
print("Print the total department in the company: ", df_department_count.count())

# Print the department name of the company
df_department_Name = df.select("department").distinct()
print("Print the department name of the company: ")
df_department_Name.show()

# Print the total employees in each department
df_employees_department = df.groupby("department").agg(count("*"))
print("Print the total employees in each department")
df_employees_department.show()

# Print the total employees in each state
df_employees_state = df.groupby("state").agg(count("*"))
print("Print the total employees in each state")
df_employees_state.show()

# Print the total employees in each state in each department
df_employees_state_department = df.groupby("state", "department").agg(count("*"))
print("Print the total employees in each state in each department")
df_employees_state_department.show()

# Print the minimum and maximum salaries in each department and sort salaries in ascending order
df_minmax_salary = df.groupBy("department").agg(min("salary").alias("min"), max("salary").alias("max")).orderBy(col("min").asc(), col("max").asc())
print("Print the minimum and maximum salaries in each department and sort salaries in ascending order:")
df_minmax_salary.show()

# Print the name of employees working in NY state in the Finance department whose bonuses are greater than the average bonus of employees in NY state
df_avg_bonus = df.filter(df["state"] == "NY").groupBy("state").agg(avg("salary").alias("avg")).collect()[0]["avg"]
df_emp_Name = df.filter((df["salary"] > df_avg_bonus) & (df["department"] == "Finance") & (df["state"] == "NY"))
print("Print the name of employees working in NY state in the Finance department whose bonuses are greater than the average bonus of employees in NY state:")
df_emp_Name.show()

# Raise the salaries 500$ of all employees whose age is greater than 45
def salary_incr(age, curr_salary):
    return curr_salary + 500 if age > 45 else curr_salary

salary_incrUDF = udf(lambda x, y: salary_incr(x, y), IntegerType())
df_update_salary = df.withColumn("salary" ,salary_incrUDF(col("age"), col("salary")))
print("Raise the salaries 500$ of all employees whose age is greater than 45:")
df_update_salary.show()

# Create DF of all those employees whose age is greater than 45
df_filter_age = df.filter(df["age"] > 45)
df_filter_age.write.mode("overwrite").options(header="True").csv("data/csvFiles/MiniProject")