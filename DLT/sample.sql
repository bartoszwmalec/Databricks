/*
Source:
https://learn.microsoft.com/en-us/training/modules/perform-incremental-processing-with-spark-structured-streaming/02-set-up-real-time-sources

Example 1:

The following SQL code snippet demonstrates how to define a Delta Live Table that reads from an Event Hubs source and performs a basic transformation:
*/
CREATE OR REPLACE STREAMING LIVE TABLE transformed_data AS
SELECT
  body AS message,
  properties['eventType'] AS event_type,
  enqueuedTime AS event_time
FROM
  cloud_files('dbfs:/mnt/event_hub_data')
  WITH FORMAT 'json'
/*
Example 2:
  
Finally, you can enhance the functionality of your incremental processing setup by integrating other features such as window functions, watermarking, and advanced SQL operations. 
  For example, you might use window functions to perform time-based aggregations on the streaming data, or watermarking to handle late-arriving data. 
  An example SQL query that uses a window function to calculate the average event value over a sliding window looks like:
*/
CREATE OR REPLACE STREAMING LIVE TABLE windowed_aggregates AS
SELECT
  event_type,
  AVG(CAST(message AS FLOAT)) OVER (PARTITION BY event_type ORDER BY event_time RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_value
FROM
  LIVE.transformed_data
