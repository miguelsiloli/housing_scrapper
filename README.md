# Airflow DAGs for Web Scraping and Data Processing

Use cases:
- Scrape raw data from source 
  - idealista scrape html files from a data catalog previously built to show the hierarchy between locations
  - imovirtual uses a reverse engineering of the graphql endpoint to get data from major locations
- Parse raw data into semi structured formats
  - idealista parses data into a flatten dataframe using lxml
  - imovirtual parses data into a flatten dataframe
- Upload the data to S3

## Cloud Architecture

![Cloud](assets/cloud_arch.png)

**Fast API repo**: https://github.com/miguelsiloli/housing_prices_fastapi
**Dash repo**: https://github.com/miguelsiloli/housing_scrapper

## Overview

This repository contains two Airflow DAGs for scraping real estate data from Idealista and Imovirtual, processing the data, and uploading it to an AWS S3 bucket.

## Project Structure

- `idealista_dag.py`: DAG for scraping and processing Idealista data.
- `imovirtual_dag.py`: DAG for fetching and processing Imovirtual data.
- more to be added

## Prerequisites

- Python 3.8+
- Apache Airflow
- AWS account with S3 access
- Required Python packages (specified in your environment setup)

## Setup

1. **Clone the repository**:
   ```bash
   git clone url.git
   cd directory

2. **Set env variables on .env file**:
  
aws_access_key_id
aws_secret_access_key
region_name


## Setup

Idealista DAG (idealista_dag.py)

    Description: Scrapes data from Idealista, processes the data, and uploads it to an S3 bucket.
    Schedule: Runs daily at 12 PM.

Tasks:

    Scrape Idealista:
        Uses a custom scraper to fetch data from provided URLs.
        URLs are read from district_data_updated.csv.
    Parse Data:
        Parses the scraped HTML files and converts them to a DataFrame.
        Uploads the parsed data to an S3 bucket.
    Delete Raw Data:
        Deletes the local folder containing raw HTML files after processing.

## Notes

### Data Engineering Workflow:

- Create a scraper and store raw data
  - Output store it either locally or in s3 bucket
- Create a parser to parse raw data into semi-structured format
  - Optimize parser (cProfile)
  - Create tests for the output data
  - Output store it in s3 bucket
- Wrap up with workflow orchestration tool
  - Define DAGs (Directed Acyclic Graphs) to manage the workflow sequence: scraping, parsing, optimizing, and testing.
  - Monitor the workflow
- Wrap up with container
- Deploy to cloud

#### Data schema

1. Raw layer (datatype = source)
2. Unnormalized Semi-structured format (data lake with parquet format)
   1. At this point its important to decompose into logical and simple tasks
3. Normalize into star schema
4. (Optional) Create SCD to track changes to listing prices if required

#### Unnormalized

| Column              | Type    | Description                                             | Constraints             |
|---------------------|---------|---------------------------------------------------------|-------------------------|
| `title`             | String  | URL of the property listing.                            | Non null.    |
| `link`              | String  | Direct link to the property listing.                    | Must be a valid URL.    |
| `description`       | String  | Description of the property.                            | None                    |
| `garage`            | Boolean | Indicates whether the property includes a garage.       | True/False values       |
| `price`             | Integer | Price of the property in euros.                         | Greater than zero.      |
| `home_size`         | String  | Describes the type of home, e.g., T1, T2, etc.          | Matches "T\d+" pattern. |
| `home_area`         | Integer | The area of the home in square meters.                  | Greater than zero.      |
| `floor`             | Integer | The floor number on which the property is located.      | Any integer.            |
| `elevator`          | Boolean | Indicates whether the building has an elevator.         | True/False values       |
| `price_per_sqr_meter` | Float | Calculated price per square meter.                     | Greater than zero.      |
| `date`              | Date    | The date associated with the data record.               | Valid date format.      |

## Star Schema Overview

### Fact Table

- **FactPropertyListings**
  - `listing_id` (PK)
  - `date_id` (FK)
  - `property_id` (FK)
  - `location_id` (FK)

### Dimension Tables

- **DimDate**
  - `date_id` (PK)
  - `day`
  - `month`
  - `year`
  - `weekday`

- **DimProperty**
  - `property_id` (PK)
  - `title`
  - `link`
  - `description`
  - `home_size`
  - `price`
  - `home_area`
  - `price_per_sqr_meter` (derived)

- **DimLocation**
  - `location_id` (PK)
  - `floor`
  - `elevator`
  - `garage`
  

### SCD 2 on table DimProperty

- **DimProperty**
  - `property_id` (PK)
  - `title`
  - `link`
  - `description`
  - `home_size`
  - `price`
  - `home_area`
  - `price_per_sqr_meter` (derived)
  - `version` (int)
  - `start_date` (date)
  - `end_date` (date)
  - `is_current` (boolean)
