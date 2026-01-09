# Job_Aggregator_ETL_Pipeline 🚀

📊 Architecture Diagram:

The architecture diagram for this project is included in this repository (architecture_diagram.drawio). It visualizes the end-to-end ETL workflow and shows how raw job data flows through the Bronze → Silver → Gold layers (Medallion Architecture) to produce clean, structured datasets and insights.

Project Overview ✨

This project is a fully functional ETL pipeline that collects job postings from Google Jobs via SerpApi, processes the data using PySpark on Databricks, and generates actionable insights on top hiring companies, trending roles, and in-demand skills.
It demonstrates a production-ready ETL workflow using Databricks’ medallion architecture, efficiently handling semi-structured API data and producing clean outputs for analysis.

Tech Stack 🛠️

• Databricks, Apache Spark, PySpark

• Python (Pandas, Requests)

• SerpApi / Google Jobs API


Approach / Methodology (Medallion Layers) 🏗️


1️⃣ Bronze Layer – Data Collection

• Fetched raw job listings via SerpApi (Google Jobs API)

• Stored raw JSON responses in the Bronze layer

2️⃣ Silver Layer – Data Transformation & Cleaning

• Converted JSON into Pandas DataFrames, then Spark DataFrames

• Cleaned nulls, duplicates, and normalized columns (job title, company, location, skills)

• Stored clean and standardized data in the Silver layer

3️⃣ Gold Layer – Aggregated Insights

• Aggregated data for analysis: job counts, top companies, locations, and trending skills

• Produced structured datasets for reporting and business insights

4️⃣ Exploratory Data Analysis (EDA) 🔍

• Analyzed top hiring companies, locations, and job categories

• Identified trending skills and roles Fetched raw job listings via SerpApi (Google Jobs API)

• Stored raw JSON responses in the Bronze layer


Key Insights / Conclusion 💡


• Dominant Roles: Data and tech jobs dominate postings

• Trending Skills: Python, SQL, Cloud Computing, and Big Data technologies

• Top Hiring Companies & Locations: Certain companies and regions have high recruitment activity

• Pipeline Efficiency: Medallion layers ensure systematic processing from raw data to actionable insights

✅ This project demonstrates API integration, PySpark transformations, Databricks workflow design, and a full ETL pipeline for job market analysis.
