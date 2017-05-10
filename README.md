# Weather_Difference_Between_SFO/Seattle

## API used:
  http://openweathermap.org/api

## Structure
  ![](/flow.png)

## Step1
* Use the requests library to pull data from api every minute
* The stream of data is shipped to S3 via kinesis using the boto3 library
* Run the above steps in an EC2 instance non-stop

## Step2
* Spin up an EMR cluster
* Using Spark to read all the data from S3
* Generate tables that are 3NF compliant and save them as parquet files in S3
* Using Spark to read the parquet files and create temp views
* Using Spark sql to select data from those views and make it a Spark DataFrame
* Turn that Spark DataFrame into a Pandas DataFrame
* Perform machine learning on that table and make a plot, save to S3
* Shut down the cluster
* Repeat the above step everyday by setting a cron job in an EC2 so new plots are generated everyday with updated data

## Step3
* Setup a Flask app that reference the plot saved in S3
* Run this app on an EC2 instance non-stop
