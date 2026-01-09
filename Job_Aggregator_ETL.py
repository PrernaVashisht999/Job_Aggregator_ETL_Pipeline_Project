# Databricks notebook source
# MAGIC %md
# MAGIC ## End-to-End Job Market Analytics ETL Pipeline using Databricks & SerpAPI

# COMMAND ----------

# MAGIC %md
# MAGIC ### Project Description

# COMMAND ----------

# MAGIC %md
# MAGIC This project demonstrates an end-to-end Job Aggregator ETL pipeline built using SerpAPI (Google Jobs API), Apache Spark, and Databricks to collect, process, and analyze software and IT job listings.
# MAGIC
# MAGIC The pipeline follows the Medallion Architecture to ensure scalable, reliable, and analytics-ready data processing:
# MAGIC
# MAGIC 🥉 Bronze Layer: Ingests raw job listings from SerpAPI and stores them in Delta tables with schema evolution support.
# MAGIC
# MAGIC 🥈 Silver Layer: Cleans and standardizes job data by handling null values, removing duplicates, and normalizing text fields to create high-quality datasets.
# MAGIC
# MAGIC 🥇 Gold Layer: Generates aggregated, business-ready tables to answer analytical questions such as top hiring roles, companies, and locations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🥉 BRONZE LAYER: Ingesting Data to Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC Fetching Software Job Listings using SerpAPI (Google Jobs API)

# COMMAND ----------

#Import Required Libraries
import requests
import json 
from pyspark.sql import SparkSession

#Create Spark Session
spark = SparkSession.builder.appName("JobETL_Bronze").getOrCreate()

#Define API & Search Parameters
API_KEY = "b1a838a281e3ca55a62d29141e876ac137c83404c2f17dc4abdb0929a9ff9839"

# Broad IT / Software job search (VERY IMPORTANT)
roles = [
    "Software Engineer",
    "IT Jobs",
    "Technology Jobs",
    "Backend Developer",    
    "Full Stack Developer",
    "Data Engineer",
    "DevOps Engineer",
    "Frontend Developer",
    "Data Scientist"
]

location = "India"

# Fetch Jobs from SerpApi (WITH PAGINATION)
all_jobs = []
print("Starting Job Fetching....\n")

for role in roles:
    print(f"Fetching jobs for role: {role}")

    next_page_token = None
    page_count = 1   # safety counter

    while True:
        print(f"  Fetching page {page_count}")

        params = {
            "engine": "google_jobs",
            "q": role,
            "location": location,
            "api_key": API_KEY
        }

        if next_page_token:
            params["next_page_token"] = next_page_token

        response = requests.get("https://serpapi.com/search.json", params=params)
        data = response.json()

        jobs = data.get("jobs_results", [])

        if not jobs:
            break

        for job in jobs:
            job["search_role"] = role
            simplified_job = {
                "title": job.get("title"),
                "company_name": job.get("company_name"),
                "location": job.get("location"),
                "via": job.get("via"),
                "share_link": job.get("share_link"),
                "search_role": role
            }
            all_jobs.append(simplified_job)

        #all_jobs.extend(jobs)

        next_page_token = data.get("next_page_token")

        if not next_page_token:
            break

        page_count += 1

print("\nJob Fetching Completed")
print(f"Total Jobs Collected: {len(all_jobs)}")


# COMMAND ----------

# MAGIC %md
# MAGIC Inspecting Fetched Job Data
# MAGIC
# MAGIC - View the keys of the first job record
# MAGIC
# MAGIC - Display a few sample jobs

# COMMAND ----------

#Viewing the structure of the first job record and a few sample jobs
print(all_jobs[0].keys())
display(all_jobs[:3])


# COMMAND ----------

# MAGIC %md
# MAGIC View RAW JSON
# MAGIC
# MAGIC - Wrap the fetched jobs in a JSON-like dictionary
# MAGIC
# MAGIC - Display total jobs and sample data

# COMMAND ----------

#View RAW JSON
job_data = {
    "total_jobs": len(all_jobs),
    "jobs": all_jobs
}

display(job_data)


# COMMAND ----------

# MAGIC %md
# MAGIC Convert to Spark DataFrame
# MAGIC
# MAGIC - Convert the Python list of dictionaries to a Spark DataFrame
# MAGIC
# MAGIC - Display the DataFrame

# COMMAND ----------

#Convert to Spark DataFrame
bronze_df = spark.createDataFrame(all_jobs)
bronze_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Save to Bronze Layer
# MAGIC
# MAGIC - Create the schema if not exists
# MAGIC
# MAGIC - Write the DataFrame as a Delta table to the Bronze layer

# COMMAND ----------

#Save to Bronze Layer
spark.sql("CREATE SCHEMA IF NOT EXISTS job_etl_bronze")

bronze_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("job_etl_bronze.jobs_raw")

print("Data written to Bronze Layer")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 🥈 Silver Layer – Data Cleaning & Standardization (Job ETL Pipeline)

# COMMAND ----------

# MAGIC %md
# MAGIC Read Raw Job Data from Bronze Layer

# COMMAND ----------

# Read Bronze Layer data
# Step 1: Read Bronze data
bronze_df = spark.table("job_etl_bronze.jobs_raw")

# Check total rows
print("Total rows in Bronze:", bronze_df.count())

# Preview data
bronze_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC Inspect Schema and Data Quality

# COMMAND ----------

# Check schema
bronze_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC Select Business-Relevant Columns

# COMMAND ----------

from pyspark.sql.functions import col

silver_df = bronze_df.select(
    col("title").alias("job_title"),
    col("company_name"),
    col("location"),
    col("via").alias("job_source"),
    col("share_link").alias("job_apply_link"),
    col("search_role")
)

silver_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Handle Null & Empty Values - Remove Null Records

# COMMAND ----------

silver_df = silver_df.dropna(
    subset=["job_title", "job_apply_link", "company_name"]
)

print("After removing nulls:", silver_df.count())


# COMMAND ----------

# MAGIC %md
# MAGIC Deduplicate Job Listings - Remove Duplicate Job Listings
# MAGIC

# COMMAND ----------

silver_df = silver_df.dropDuplicates(
    ["job_title", "company_name", "job_apply_link"]
)

print("After removing duplicates:", silver_df.count())


# COMMAND ----------

# MAGIC %md
# MAGIC Basic Data Standardization - Standardize Text Columns

# COMMAND ----------

from pyspark.sql.functions import trim, lower

silver_df = silver_df.withColumn("job_title", trim(col("job_title"))) \
                     .withColumn("company_name", trim(col("company_name"))) \
                     .withColumn("search_role", lower(col("search_role")))
silver_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Write Cleaned Data to Silver Layer

# COMMAND ----------

# Create Silver schema
spark.sql("CREATE SCHEMA IF NOT EXISTS job_etl_silver")

# Write Silver table
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("job_etl_silver.jobs_clean")

print("Data written to Silver Layer")


# COMMAND ----------

# MAGIC %md
# MAGIC ###  🥇 Gold Layer: Business Aggregations & Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC Read Data from Silver Layer

# COMMAND ----------

# Read Silver layer data
silver_df = spark.read.table("job_etl_silver.jobs_clean")

print("Total records in Silver:", silver_df.count())
silver_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Jobs per Role in Top 5 Locations

# COMMAND ----------

# MAGIC %md
# MAGIC Which roles are most in demand in the top 5 cities?

# COMMAND ----------

from pyspark.sql.functions import col

# Top 5 locations
top_locations = spark.table("job_etl_gold.jobs_by_location") \
                     .orderBy("total_jobs", ascending=False) \
                     .limit(5) \
                     .select("location")

# Filter jobs in top locations and count per role
spark.table("job_etl_silver.jobs_clean") \
     .join(top_locations, "location") \
     .groupBy("search_role", "location") \
     .count() \
     .orderBy("count", ascending=False) \
     .display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Companies Hiring for a Specific Role

# COMMAND ----------

# MAGIC %md
# MAGIC Which companies are hiring the most for ‘Data Engineer’?

# COMMAND ----------

role = "data engineer"

spark.table("job_etl_silver.jobs_clean") \
     .filter(col("search_role") == role) \
     .groupBy("company_name") \
     .count() \
     .orderBy("count", ascending=False) \
     .display()
 



# COMMAND ----------

# MAGIC %md
# MAGIC ## Roles Available in a Specific Location

# COMMAND ----------

# MAGIC %md
# MAGIC What roles are available in Hyderabad?

# COMMAND ----------

# Import required function
from pyspark.sql.functions import lower, col

# Set your location
location = "hyderabad, telangana, india"

# Query Gold insights: Jobs per role in this location
spark.table("job_etl_silver.jobs_clean") \
     .filter(lower(col("location")) == location.lower()) \
     .groupBy("search_role") \
     .count() \
     .orderBy("count", ascending=False) \
     .display()



# COMMAND ----------

# MAGIC %md
# MAGIC ## Companies with Most Diverse Roles

# COMMAND ----------

# MAGIC %md
# MAGIC Which companies are hiring for the most different types of roles?

# COMMAND ----------

from pyspark.sql.functions import countDistinct, col

# Top companies by number of unique roles
spark.table("job_etl_silver.jobs_clean") \
     .groupBy("company_name") \
     .agg(countDistinct("search_role").alias("unique_roles")) \
     .orderBy("unique_roles", ascending=False) \
     .display()



# COMMAND ----------

# MAGIC %md
# MAGIC ## Roles That Have Both High Hiring and High Locations

# COMMAND ----------

# MAGIC %md
# MAGIC Which roles appear in the most locations and have high job counts?

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, col

spark.table("job_etl_silver.jobs_clean") \
     .groupBy("search_role") \
     .agg(
         count("job_title").alias("total_jobs"),
         countDistinct("location").alias("locations_count")
     ) \
     .orderBy(col("total_jobs").desc(), col("locations_count").desc()) \
     .display()



# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Aggregated Job Insights to the Gold Layer

# COMMAND ----------

#Gold Layer: Persisting Aggregated Job Analytics Tables
# Create Gold schema
spark.sql("CREATE SCHEMA IF NOT EXISTS job_etl_gold")

# Save jobs by role
jobs_by_role.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("job_etl_gold.jobs_by_role")

# Save jobs by company
jobs_by_company.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("job_etl_gold.jobs_by_company")

# Save jobs by location
jobs_by_location.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("job_etl_gold.jobs_by_location")

print("All Gold Layer tables saved successfully")



# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 Creating Final Gold Jobs Table

# COMMAND ----------

from pyspark.sql.functions import col

final_gold_df = spark.table("job_etl_silver.jobs_clean") \
    .select(
        col("job_title"),
        col("company_name"),
        col("location"),
        col("search_role"),
        col("job_apply_link"),
        col("job_source")
    )

final_gold_df.display()

#Save This as Gold Table
final_gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("job_etl_gold.jobs_final")

print("Final Gold Jobs table saved successfully")



# COMMAND ----------

# MAGIC %md
# MAGIC ## View Gold table using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM job_etl_gold.jobs_final;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusion

# COMMAND ----------

# MAGIC %md
# MAGIC The ETL pipeline successfully ingested, processed, and analyzed software and IT job listings using SerpAPI, Apache Spark, and Databricks.
# MAGIC
# MAGIC Key business insights derived from the Gold layer include:
# MAGIC
# MAGIC - Roles with the most job openings: Data Analyst, Software Engineer, Backend Developer, and Full Stack Developer
# MAGIC
# MAGIC - Top hiring companies: Accenture, Infosys, Cognizant, Oracle, and TCS
# MAGIC
# MAGIC - Top job locations: Hyderabad, Bangalore, and pan-India/remote locations
# MAGIC
# MAGIC Additionally, roles such as Data Analyst and Data Engineer show both high hiring volume and wide geographic distribution, indicating strong and sustained demand across multiple regions. Hyderabad emerged as a major hiring hub, particularly for analytics and engineering roles.
# MAGIC
# MAGIC This project demonstrates how the Medallion Architecture enables clean, standardized, and analytics-ready datasets, empowering organizations to efficiently answer critical hiring, workforce planning, and job market intelligence questions.