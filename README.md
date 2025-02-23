
### Weather ETL

#### Project Overview
This is a Data Engineering project where we implement an ETL (Extract, Transform, Load) pipeline to process and manage large datasets. The project uses several modern technologies to handle data extraction, transformation, and loading processes efficiently. 

#### The primary technologies used in this project are:

- Apache Spark: Used for distributed data processing and transformation.
- PostgreSQL: A relational database used to store the extracted data.
- Amazon Redshift: A data warehousing solution used to store the transformed and processed data for analytics and reporting.

#### Project Structure
- *Extract*:
The data is extracted from a PostgreSQL database using ORM.
Data is extracted in raw form and stored temporarily for transformation.

- *Transform*:
Apache Spark is used to process and clean the raw data.
Various data transformations, including filtering, aggregation, are applied to prepare the data for loading into the destination system.

- *Load*:
The transformed data is loaded into an Amazon Redshift data warehouse.
The data is structured and optimized for fast querying and reporting.

#### Technologies Used
- **Apache Spark**: Distributed data processing framework for transforming data.
- **PostgreSQL**: Open-source relational database for storing raw data.
- **Amazon Redshift**: Cloud-based data warehousing service for storing the processed data and enabling analytics.
- **Python**: Used for scripting and orchestration of the ETL pipeline.
- **SQL**: For querying and manipulating data in PostgreSQL and Redshift.
