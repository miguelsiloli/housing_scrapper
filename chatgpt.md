1. Use Case Diagram

Context: Developing an application to parse website data using Selenium, store raw HTML files, convert them into Parquet files, upload to S3, retrain a model with MLflow, and serve the model using FastAPI.

Actors:

    User: Initiates the scraping and model training processes.
    System: The backend system handling the scraping, processing, uploading, training, and serving.

Use Cases:

    Start Scraping
    Store Raw HTML
    Convert HTML to Parquet
    Upload Parquet to S3
    Retrain Model
    Serve Model1. Use Case Diagram

Context: Developing an application to parse website data using Selenium, store raw HTML files, convert them into Parquet files, upload to S3, retrain a model with MLflow, and serve the model using FastAPI.

Actors:

    User: Initiates the scraping and model training processes.
    System: The backend system handling the scraping, processing, uploading, training, and serving.

Use Cases:

    Start Scraping
    Store Raw HTML
    Convert HTML to Parquet
    Upload Parquet to S3
    Retrain Model
    Serve Model


```
                   +----------------------+
                   |        System        |
                   +----------------------+
                          ^    ^
                          |    |
                          |    |
         +---------------+|    |+---------------+
         |                |    |                |
         |                |    |                |
+--------+-----+   +------+----+-----+   +------+-+
|     User     |   |     Start Scraping      |   |    |
+--------------+   +--------------------+   +------------+
      |                  |                ^
      |                  |                |
      +---- Store Raw HTML  +---+
      |                  |                ^
      |                  |                |
      +---- Convert HTML to Parquet +----+
      |                  |                ^
      |                  |                |
      +---- Upload Parquet to S3 +--------+
      |                  |                ^
      |                  |                |
      +---- Retrain Model +--------------+
      |                  |                ^
      |                  |                |
      +---- Serve Model +-----------------+
```

2. Revised Class Diagram

Context: Developing an application to parse website data using Selenium, store raw HTML files, convert them into Parquet files, upload to S3, retrain a model with MLflow, and serve the model using FastAPI.

Classes:

    Scraper
    Storage
    ParquetConverter
    MLFlowTrainer
    ModelService

```
+--------------------+
|      Scraper       |
+--------------------+
| -url               |
| -driver            |
+--------------------+
| +startScraping()   |
| +getHTML()         |
+--------------------+
         |
         v
+--------------------+
|      Storage       |
+--------------------+
| -filePath          |
| -s3Bucket          |
+--------------------+
| +storeHTML()       |
| +uploadFile()      |
+--------------------+
         |
         v
+--------------------+
|  ParquetConverter  |
+--------------------+
| -htmlFilePath      |
| -parquetFilePath   |
+--------------------+
| +convertToParquet()|
+--------------------+
         |
         v
+--------------------+
|   MLFlowTrainer    |
+--------------------+
| -parquetFilePath   |
| -modelPath         |
+--------------------+
| +trainModel()      |
+--------------------+
         |
         v
+--------------------+
|   ModelService     |
+--------------------+
| -modelPath         |
+--------------------+
| +loadModel()       |
| +serveModel()      |
+--------------------+
```

3. Sequence Diagram

```
Scheduler            Scraper            Storage            ParquetConverter          MLFlowTrainer          S3            ModelService
  |                |                  |                     |                        |                   |                |
  |  startScraping()|                  |                     |                        |                   |                |
  |---------------->|                  |                     |                        |                   |                |
  |                | getHTML()         |                     |                        |                   |                |
  |                |-----------------> |                     |                        |                   |                |
  |                | storeHTML(html)   |                     |                        |                   |                |
  |                |------------------>|                     |                        |                   |                |
  |                |                  | uploadFile()         |                        |                   |                |
  |                |                  |--------------------->|                        |                   |                |
  |                |                  |                     |                        |                   |                |
  |                |                  |                     | convertToParquet()     |                   |                |
  |                |                  |                     |------------------------>|                   |                |
  |                |                  |                     |                        | uploadParquet()   |                |
  |                |                  |                     |                        |------------------>|                |
  |                |                  |                     |                        |                   |                |
  |                |                  |                     |                        | retrainModel()    |                |
  |                |                  |                     |                        |------------------>| saveModel()    |
  |                |                  |                     |                        |                   |--------------->|
  |                |                  |                     |                        |                   |                |
  |                |                  |                     |                        |                   | getModel()     |
  |                |                  |                     |                        |                   |<---------------|
  |                |                  |                     |                        |                   |                |
  |                |                  |                     |                        |                   | serveModel()   |
  |                |                  |                     |                        |                   |<---------------|
  |                |                  |                     |                        |                   |                |
  |                |                  |                     |                        |                   |                |
```

4. Activity Diagram

```
+---------------------+
|      Start          |
+---------------------+
          |
          v
+---------------------+
|  Start Scraping     |
+---------------------+
          |
          v
+---------------------+
| Store HTML Locally  |
+---------------------+
          |
          v
+---------------------+
| Upload HTML to S3   |
+---------------------+
          |
          v
+---------------------+
| Convert HTML to     |
| Parquet             |
+---------------------+
          |
          v
+---------------------+
| Upload Parquet to S3|
+---------------------+
          |
          v
+---------------------+
| Retrain Model       |
+---------------------+
          |
          v
+---------------------+
| Save Model to S3    |
+---------------------+
          |
          v
+---------------------+
| Serve Model         |
+---------------------+
          |
          v
+---------------------+
|         End         |
+---------------------+
```

7. Deployment Diagram

```
+-------------------+
|   User Device     |
+-------------------+
         |
         v
+-------------------+
|    Web Server     |
| - User Interface  |
+-------------------+
         |
         v
+-------------------+      +-------------------+
| Application Server|      |    S3 Bucket      |
| - Scraper         |      | - HTML Files      |
| - Storage         |      | - Parquet Files   |
| - Parquet Conv.   |      | - Models          |
| - MLFlow Trainer  |      +-------------------+
| - Model Service   |
+-------------------+
         |
         v
+-------------------+
| Database Server   |  (optional)
| - Metadata        |
| - Logs            |
+-------------------+
```

8. Data Flow Diagram (DFD)

```
+---------+        +----------------+        +---------+
|  User   |        |     System     |        |   S3    |
+---------+        +----------------+        +---------+
      |                    |                      |
      | startScraping()    |                      |
      |------------------->|                      |
      |                    | getHTML()            |
      |                    |--------------------->|
      |                    | storeHTML()          |
      |                    |<---------------------|
      |                    |                      |
      |                    | convertToParquet()   |
      |                    |--------------------->|
      |                    | uploadParquet()      |
      |                    |<---------------------|
      |                    |                      |
      |                    | retrainModel()       |
      |                    |--------------------->|
      |                    | saveModel()          |
      |                    |<---------------------|
      |                    |                      |
      |                    | serveModel()         |
      |<-------------------| getModel()           |
      |                    |--------------------->|
```