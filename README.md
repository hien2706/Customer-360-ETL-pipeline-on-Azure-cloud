# Customer-360-ETL-pipeline-on-Azure-cloud
# Overview
The pipeline extracts raw data from Blob Storage, transforms it using PySpark on Databricks, loads it into SQL Database, and visualizes the results in Power BI. Azure Data Factory automates the entire process.

Technologies used:
- PySpark
- Azure Databricks
- Azure Blob Storage
- Azure SQL Database
- Azure Data Factory
- Power BI

### Project Details
Azure Data Factory is used to automate and schedule databrick job
#### Extract
log_content folder(interaction data), log_search folder(behavior data), and mapping file are mounted into azure Databricks .

#### Transform
PySpark is utilized to transform the data.
##### Interaction data
Steps included:
- Categorize the `AppName` column and calculate the total duration each contract has with each category.
- Calculate the total devices each contract uses.
- Identify the type each contract watches the most.
- Determine the various types each contract watches.
- Assess the activeness of each contract.
- Segment Customers into categories.

Before Transformation:\
![interaction_data_before](https://github.com/hien2706/Customer360/blob/main/pictures/interaction_data_before.png)\
After Transformation:\
![interaction_data_after](https://github.com/hien2706/Customer360/blob/main/pictures/interaction_data_after.png)

##### Behavior data
Steps included:
- Filter out NULL values and data from months 6 and 7.
- Identify the most searched keyword for each `user_id` in months 6 and 7.
- Categorize the most searched keyword for each user.
- Calculate a new column `Trending_Type` to identify if the category changed or remained unchanged within 2 months.
- Calculate a new column `Previous` to show any changes in category within 2 months, if applicable.
  
Before Transformation:\
![behavior_data_before](https://github.com/hien2706/Customer360/blob/main/pictures/behavior_data_before.png)\
After Transformation:\
![behavior_data_after](https://github.com/hien2706/Customer360/blob/main/pictures/behavior_data_after.png)

Finally, the transformed log_content and log_search datasets are unionized based on user_id and contract. \
Final result: \
![final_result](https://github.com/hien2706/Customer360/blob/main/pictures/customer_data_final.png)

#### Load
The transformed data is saved into csv file and loaded into Azure SQL Database.

#### Analyze
PowerBI is used to create dashboards .\
![Dashboard](https://github.com/hien2706/Customer360/blob/main/pictures/customer_data.pdf):\
![DashBoard](https://github.com/hien2706/Customer360/blob/main/pictures/dashboard.png)
