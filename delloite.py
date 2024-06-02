+------+--------+---------+----------+
|  id  | name   | Grade   |  Salary  |
+======+========+=========+==========+
|  1   | Rohit  | B       |  16000   |
+------+--------+---------+----------+
|  2   | Virat  | A       |  20000   |
+------+--------+---------+----------+
|  3   | Dhoni  | A       |  25000   |
+------+--------+---------+----------+
|  4   | Ashwin | C       |  12000   |
+------+--------+---------+----------+
|  5   | Zahir  | C       |  13000   |
+------+--------+---------+----------+

with employee_cte as(
select *, rank() over(order by Salary desc) as rnk from employee)
select * from employee_cte where rnk=3;

Weather table:
   +----+------------+-------------+
   | id | recordDate | temperature |prev_temp
   +----+------------+-------------+
   | 1  | 2015-01-01 | 10          |null
   | 2  | 2015-01-02 | 25          |10
   | 3  | 2015-01-03 | 20          |25
   | 4  | 2015-01-04 | 30          |20
   +----+------------+-------------+
   
with Weather_cte as(
select *, lag(temperature, 1, 0) as prev_temp from Weather)
select id from Weather_cte where temperature > prev_temp;


Total number of employees whose salary is greater than the average salary
code in pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Create a Spark session
spark = SparkSession.builder \
    .appName("AverageSalaryAndCount") \
    .getOrCreate()

# Assume you have a DataFrame named 'employees_df' containing employee data
# Load your employee data into 'employees_df' using your actual data source
# employees_df = spark.read.format("csv").option("header", "true").load("path_to_your_data.csv")

# Calculate the average salary
average_salary = employees_df.agg(avg("salary")).collect()[0][0]

# Filter employees whose salary is greater than the average salary
filtered_df = employees_df.filter(employees_df["salary"] > average_salary)

# Count the number of employees meeting the criteria
total_employees_above_avg_salary = filtered_df.count()

# Print the total number of employees above average salary
print("Total number of employees whose salary is greater than the average salary:", total_employees_above_avg_salary)

# Stop the Spark session
spark.stop()

i/p: 1222311
o/p: (1, 1) (2, 3) (3, 1) (1, 2)

str = "1222311"
res = ""
cnt = 1
for i in range(len(str)-1):
    if str[i] == str[i+1]:
        cnt+=1
    else:
        print(tuple((int(str[i]), cnt)))
        cnt = 1
print(tuple((int(str[i]), cnt)))

#str = "1222311"
#cnt = 1
#for i in (len(str)-1):
#	if str[i] == str[i+1]:
#		cnt++
#	else:
#		print(tuple((str[i], cnt)))
		

how to partition on table with columns: eid, ename, salary, deptid, joiningdate, city 

ingestion time partitioning
create table table_name (eid int, ename string)
partition by DATE(_PARTITIONTIME)
cluster by deptid

data fusion?
authorized views?
materialized views?

how to load data from bq to gcs?
how to recover data in bq?
10 days data in bq table lost
staging
time travel
-------------------------------------------------------BQ time travel
Time travel in BigQuery refers to the ability to query data as it existed at different points in time, allowing you to analyze historical data or compare changes over time without relying on manual backups or snapshots.

   - Time travel is limited by the retention period of data in BigQuery. By default, BigQuery retains data for 365 days, but this can be adjusted based on your storage plan.

   - Query data as it existed one day ago:

     SELECT *
     FROM my_dataset.my_table
     FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY);
     #to query specific date time 
     #FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 00:00:00';
----------------------------------------------BQ undelete
If data has been accidentally deleted from Google BigQuery, you can recover it under certain conditions. BigQuery provides a feature called "undelete" that allows you to recover tables, views, and partitions within a specific time frame after deletion.
### Steps to Recover Deleted Data:

1. *Navigate to the Deleted Data Section:*
   - Go to the BigQuery web UI.
   - On the left-hand side, click on the project containing the deleted data.
   - Click on the "Deleted Data" section. If you don't see this section, make sure you have sufficient permissions.

2. *Find the Deleted Data:*
   - In the "Deleted Data" section, you'll see a list of datasets and tables that have been recently deleted.
   - Locate the dataset or table that contains the data you want to recover.

3. *Undelete the Data:*
   - Click on the dataset or table that you want to recover.
   - In the details panel, you'll see an option to "Undelete" the dataset or table. Click on this option.

You can only undelete data within a specific time frame after deletion. The default time frame is 7 days, but this can be extended up to 365 days for some Google Cloud projects.
- *Permissions:* You need sufficient permissions (e.g., roles with the bigquery.tables.update permission) to undelete data in BigQuery.
- *Data Integrity:* Undeleting data restores the entire dataset or table to its state before deletion. If any modifications were made after deletion, they will not be recovered.
- *Partitioned Tables:* If you are recovering a partitioned table, you can select specific partitions to undelete rather than the entire table.
-------------------------------------------authorized view
an authorized view is a virtual table that references a query and allows specific users or groups to access the results of that query without giving direct access to the underlying tables or data. This is useful for sharing data securely while controlling access to sensitive information.

Here's how you can create an authorized view in BigQuery:

1. *Create a View:*
2. *Authorize Access:*
   authorize specific users, groups, or service accounts to access it. You can do this through BigQuery's IAM (Identity and Access Management) settings.

   - Go to the BigQuery web UI.
   - Navigate to your project and dataset containing the view.
   - Click on the "Share Dataset" button.
   - Add the email addresses or groups of users who should have access to the view and specify their access level (e.g., Viewer, Editor, or Owner).

3. *Access the Authorized View:*
   Users who have been granted access can then query the authorized view as if it were a regular table. They do not need direct access to the underlying tables used in the view.

   SELECT * FROM project_id.dataset_id.view_name;