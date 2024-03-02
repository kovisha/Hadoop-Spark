from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import time

S3_DATA_SOURCE_PATH = 's3://airlinesbigdataassignment/DataSource/DelayedFlights.csv'

def spark_execution():
    spark = SparkSession.builder.appName("flight_delay").getOrCreate()
    flight_delay_df = spark.read.csv(S3_DATA_SOURCE_PATH, header=True, inferSchema=True)
    flight_delay_df.createOrReplaceTempView("delay_flights")
    start_time = time.perf_counter()
    result_df = spark.sql(f"""
                        SELECT Year, avg((NASDelay / ArrDelay) * 100) AS YearWise_NASDelay
                        FROM delay_flights
                        WHERE year BETWEEN 2003 AND 2010
                        GROUP BY Year
                        ORDER BY Year DESC
                    """)
    end_time = time.perf_counter()
    result_df.show()
    execution_time = (end_time - start_time)
    print(f"Execution time: {execution_time} seconds ")

if __name__ == '__main__':
    spark_execution()
