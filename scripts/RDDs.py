from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("RDD")
sc = SparkContext(conf=conf)

rdd = sc.textFile("data/test.txt")
print(rdd.collect())

# map()
# Split each line into words
# and create a new RDD with the results
rdd_map = rdd.map(lambda line: line.split(' '))
print("Mapped RDD:")
print(rdd_map.collect())

def foo(line):
    l1 = line.split(' ')
    l2 = [int(x) + 2 for x in l1]
    return l2

rdd_map = rdd.map(foo)
print("Mapped RDD with custom function:")
print(rdd_map.collect())

# Quiz 1:
rdd = sc.textFile("data/quiz1.txt")
rdd_map_quiz1 = rdd.map(lambda x: [len(word) for word in x.split(' ')])
print("Quiz 1 Mapped RDD:")
print(rdd_map_quiz1.collect())


# flatMap()
# Split each line into words
# and create a new RDD with the results
rdd = sc.textFile("data/test.txt")
rdd_flatmap = rdd.flatMap(lambda line: line.split(' '))
print("FlatMapped RDD:")
print(rdd_flatmap.collect())


# filter()
# Filter out lines that do not contain the word '4'
rdd = sc.textFile("data/test.txt")
rdd_filter = rdd.filter(lambda line: '4' in line)
print("Filtered RDD:")
print(rdd_filter.collect())

# Quiz 2:
rdd = sc.textFile("data/Quiz2.txt")
rdd_map = rdd.flatMap(lambda x: x.split(' '))
print(rdd_map.collect())

rdd_filter_quiz2 = rdd_map.filter(lambda x: x[0] != 'a' and x[0] != 'c')
print("Quiz 2 Filtered RDD:")
print(rdd_filter_quiz2.collect())


# distinct()
# Get distinct words from the RDD
rdd = sc.textFile("data/test.txt")
rdd_distinct = rdd.flatMap(lambda line: line.split(' ')).distinct()
print("Distinct RDD:")
print(rdd_distinct.collect())

# groupByKey() mapValues()
rdd = sc.textFile("data/Quiz2.txt")
rdd_map = rdd.flatMap(lambda x : x.split(' '))
rdd_map = rdd_map.map(lambda x: (x, len(x)))
rdd_grouped = rdd_map.groupByKey().mapValues(list)
print("Grouped RDD:")
print(rdd_grouped.collect())


# reduceByKey()
rdd = sc.textFile("data/test.txt")
rdd_map = rdd.flatMap(lambda line: line.split(' '))
rdd_map = rdd_map.map(lambda x: (x, 1))
rdd_reduced = rdd_map.reduceByKey(lambda a, b: a + b)
print("Reduced RDD:")
print(rdd_reduced.collect())

# Quiz 3:
rdd = sc.textFile("data/Quiz3.txt")
rdd_map = rdd.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1))
rdd_reduced_quiz3 = rdd_map.reduceByKey(lambda a, b: a + b)
print("Quiz 3 Reduced RDD:")
print(rdd_reduced_quiz3.collect())

# count() countByValue()
rdd = sc.textFile("data/test.txt")
rdd_map = rdd.flatMap(lambda line: line.split(' '))
rdd_count = rdd_map.count()
print("Count of words in RDD:", rdd_count)
rdd_count_by_value = rdd_map.countByValue()
print("Count by value of words in RDD:")
print("Partition count:", rdd_map.getNumPartitions())

# SaveAsTextFile
try:
    rdd = sc.textFile("data/test.txt")
    rdd_map = rdd.flatMap(lambda line: line.split(' '))
    rdd_map.saveAsTextFile("data/words_output")
    print(rdd_map.getNumPartitions())
except Exception as e:
    print("Error saving RDD to text file")

# repartition() increase/decrease partitions
rdd = sc.textFile("data/test.txt")
rdd_repartitioned = rdd.repartition(3)
print("Repartitioned RDD:", rdd_repartitioned.getNumPartitions())

# coalesce() decrease partitions
rdd_coalesced = rdd.coalesce(1)
print("Coalesced RDD:", rdd_coalesced.getNumPartitions())


# Avarage
rdd = sc.textFile("data/csvFiles/movie_ratings.csv")
rdd_map = rdd.map(lambda line: (line.split(',')[0], float(line.split(',')[1])))
rdd_map = rdd_map.map(lambda x: (x[0], (x[1], 1)))  
rdd_reduces = rdd_map.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
rdd_average = rdd_reduces.map(lambda x: (x[0], x[1][0] / x[1][1])) 
print("Average Ratings:")
print(rdd_average.collect())

# Quiz 4: Average Ratings
rdd = sc.textFile("data/csvFiles/average_quiz_sample.csv")
rdd_map = rdd.map(lambda line: (line.split(',')[0], (float(line.split(',')[2]), 1)))
rdd_reduces = rdd_map.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
rdd_average = rdd_reduces.map(lambda x: (x[0], x[1][0] / x[1][1]))
print("Quiz 4 Average Ratings:")
print(rdd_average.collect())

# min() max()
rdd = sc.textFile("data/csvFiles/movie_ratings.csv")
rdd_map = rdd.map(lambda line: (line.split(',')[0], float(line.split(',')[1])))
rdd_min = rdd_map.reduceByKey(lambda a, b: a if a < b else b)
rdd_max = rdd_map.reduceByKey(lambda a, b: a if a > b else b)
print("Minimum Rating:", rdd_min.collect())
print("Maximum Rating:", rdd_max.collect())


# Quiz 5: Min and Max Ratings
rdd = sc.textFile("data/csvFiles/average_quiz_sample.csv")
rdd_map = rdd.map(lambda line: (line.split(',')[1], float(line.split(',')[2])))
rdd_min_quiz5 = rdd_map.reduceByKey(lambda a, b: a if a < b else b)
rdd_max_quiz5 = rdd_map.reduceByKey(lambda a, b: a if a > b else b)
print("Quiz 5 Minimum Ratings:")
print(rdd_min_quiz5.collect())
print("Quiz 5 Maximum Ratings:")
print(rdd_max_quiz5.collect())


# Mini project: Students
rdd = sc.textFile('data/csvFiles/StudentData.csv')
rdd = rdd.map(lambda x: x.split(','))
header = rdd.first()  # Get the header
rdd = rdd.filter(lambda line: line != header)  # Remove the header from the RDD

# Show the number of student in the dataset
num_students = rdd.count()
print("Number of students in the dataset:", num_students)

# Show the total marks achived by Female and Male students
Female_mark = rdd.map(lambda x: (x[1], int(x[5]))).filter(lambda x: x[0] == 'Female').reduceByKey(lambda a, b: a + b)
print("Total marks achieved by female students:", Female_mark.collect())
Male_mark = rdd.map(lambda x: (x[1], int(x[5]))).filter(lambda x: x[0] == 'Male').reduceByKey(lambda a, b: a + b)
print("Total marks achieved by male students", Male_mark.collect())

# Show the total number of students that have passed and failed. 50+ marks is required to pass the course
passed_students = rdd.map(lambda x: int(x[5])).filter(lambda x: x != 'marks' and x >= 50).count()
failed_students = rdd.map(lambda x: int(x[5])).filter(lambda x: x != 'marks' and x < 50).count()
print("Total number of students that have passed:", passed_students)
print("Total number of students that have failed:", failed_students)

# Show the total number of students enrolled in each course
course_enrollment = rdd.map(lambda x: (x[3], 1)).filter(lambda x : x[0] != 'course').reduceByKey(lambda a, b: a + b)
print("Total number of students enrolled in each course:")
print(course_enrollment.collect())

# Show the total marks that students have achieved in each course
course_marks = rdd.map(lambda x: (x[3], int(x[5]))).reduceByKey(lambda a, b: a + b)
print("Total marks achieved by students in each course:")
print(course_marks.collect())

# Show the average marks achieved by students in each course
course_avg_marks = rdd.map(lambda x: (x[3], (int(x[5]), 1))).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda x: (x[0], x[1][0] / x[1][1]))
print("Average marks achieved by students in each course:")
print(course_avg_marks.collect())

# Show the minimum and maximum marks achieved by students in each course
course_min_marks = rdd.map(lambda x: (x[3], int(x[5]))).reduceByKey(lambda a, b: a if a < b else b)
course_max_marks = rdd.map(lambda x: (x[3], int(x[5]))).reduceByKey(lambda a, b: a if a > b else b)
print("Minimum marks achieved by students in each course:")
print(course_min_marks.collect())
print("Maximum marks achieved by students in each course:")
print(course_max_marks.collect())

# Show the average age of male and female students
avg_age_female = rdd.map(lambda x: (x[1], (int(x[0]), 1))).filter(lambda x: x[0] == 'Female').reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda x: (x[0], x[1][0] / x[1][1]))
avg_age_male = rdd.map(lambda x: (x[1], (int(x[0]), 1))).filter(lambda x: x[0] == 'Male').reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda x: (x[0], x[1][0] / x[1][1]))
print("Average age of female students:")
print(avg_age_female.collect())
print("Average age of male students:")
print(avg_age_male.collect())