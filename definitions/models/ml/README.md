# README: ML Pipeline with BigQuery ML and Dataform

## Overview

This project demonstrates how to build a machine learning (ML) pipeline using **BigQuery ML** within a **Dataform** environment. We use a public dataset of **Chicago Taxi Trips** to train, evaluate, and predict fare amounts based on features like trip duration, distance, and payment type. The ML models are trained and executed within **Google BigQuery** using SQL, and the overall workflow is managed with **Dataform** for version control and reproducibility.

## Files and Structure

### 1. **Data Extraction**
   - We prepare three datasets: training, evaluation, and testing, by applying necessary transformations and feature engineering on the raw Chicago Taxi Trips data.
   - **Files:**
     - **`definitions/models/ml/1_extraction/chicago_taxi_trips.sqlx`**:
       Declares the public dataset as a source.
       ```sql
       config {
           type: "declaration",
           database: "bigquery-public-data",
           schema: "chicago_taxi_trips",
           name: "taxi_trips"
       }
       ```

     - **`definitions/models/ml/1_extraction/chicago_taxi_trips_training.sqlx`**:
       Prepares the **training dataset** by selecting trips between 2 and 5 months before the most recent date in the dataset and applying filtering and transformations.
       ```sql
       config {
           type: "table",
           schema: "df_bqml_chicago_taxi_trips"
       }

       -- Transforms and filters data for training
       ```

     - **`definitions/models/ml/1_extraction/chicago_taxi_trips_evaluation.sqlx`**:
       Extracts the **evaluation dataset**, selecting trips between 1 and 2 months before the most recent date, following similar filtering and transformation steps as the training set.
       ```sql
       config {
           type: "table",
           schema: "df_bqml_chicago_taxi_trips"
       }
       
       -- Transforms and filters data for evaluation
       ```

     - **`definitions/models/ml/1_extraction/chicago_taxi_trips_testing.sqlx`**:
       Prepares the **testing dataset**, using the most recent month of trips. This dataset is used for model prediction after training.
       ```sql
       config {
           type: "table",
           schema: "df_bqml_chicago_taxi_trips"
       }
       
       -- Transforms and filters data for testing
       ```

### 2. **Model Training**
   - The machine learning model is trained using **BigQuery ML**. Specifically, we implement a **DNN Regressor** (Deep Neural Network) model to predict the total fare based on various features extracted from the taxi trips.
   - **Files:**
     - **`definitions/models/ml/2_training/dnn_model_train.slqx`**:
       Trains a **Deep Neural Network (DNN)** regressor model with the extracted training dataset. The target label is the fare, including tips, tolls, and extras.
       ```sql
       CREATE OR REPLACE MODEL ${self()}  
       OPTIONS (
          MODEL_TYPE = 'DNN_REGRESSOR',
          ACTIVATION_FN = 'RELU',
          HIDDEN_UNITS = [64, 64, 64, 64],
          INPUT_LABEL_COLS = ['label']
         )
       ```

### 3. **Model Evaluation**
   - Once the model is trained, we evaluate its performance using the **ML.EVALUATE** function in BigQuery ML. The evaluation focuses on metrics like **mean absolute error** (MAE), and assertions are added to validate model performance.
   - **Files:**
     - **`definitions/models/assertions/model_evaluate.sqlx`**:
       Evaluates the performance of the trained model on the evaluation dataset. The assertion ensures that the mean absolute error (MAE) is below a specified threshold.
       ```sql
       SELECT
          *
       FROM
          ML.EVALUATE(
             MODEL ${ref('dnn_reg')}, 
             (
                SELECT * FROM ${ref('chicago_taxi_trips_evaluation')}
             )
          )
       WHERE mean_absolute_error > 5
       ```

### 4. **Model Prediction**
   - Finally, the trained model is used to predict fares on the testing dataset. The predicted values are rounded and included in the output.
   - **Files:**
     - **`definitions/models/ml/3_prediction/predict.sqlx`**:
       Uses the trained **DNN Regressor** to predict fares on the testing dataset and rounds the predictions for better readability.
       ```sql
          SELECT
             dayofweek,
             hourofday,
             trip_distance,
             trip_miles,
             trip_seconds,
             payment_type,
             company,
             ROUND(predicted_label, 2) AS predicted_fare
          FROM
             ML.PREDICT(
                MODEL ${ref('dnn_reg')},
                (
                   SELECT * FROM ${ref('chicago_taxi_trips_testing')}
                )
             )
       ```

## Workflow and Pipeline

1. **Data Extraction**: Extracts and transforms the Chicago Taxi Trips data from BigQuery for three datasets: training, evaluation, and testing.
2. **Model Training**: A DNN model is trained using the training dataset to predict the total fare amount.
3. **Model Evaluation**: The model is evaluated on the evaluation dataset, and an assertion is made to ensure acceptable model performance.
4. **Model Prediction**: The model is used to predict fares on the testing dataset, and the results are output as a new table.

## Technologies Used

- **Dataform**: Manages the SQL transformations and model training as part of a reproducible and version-controlled pipeline.
- **BigQuery ML**: Used for training, evaluating, and predicting with machine learning models directly within BigQuery using SQL.
- **SQL**: All transformations, data extraction, and model training are done using SQL scripts managed by Dataform.

## Running the Pipeline

To execute the pipeline, ensure you have access to the **Google BigQuery** project and **Dataform** workspace. The entire process from data extraction to prediction is automated through Dataform's environment, ensuring a fully managed ML pipeline.

## Conclusion

This project showcases how to leverage **BigQuery ML** and **Dataform** to implement a fully automated ML pipeline for predicting taxi fares in Chicago. The combination of data extraction, transformation, model training, evaluation, and prediction in a single pipeline makes it easy to manage and scale ML workflows in a cloud-based environment.
