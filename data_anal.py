from pyspark.sql import SparkSession
from pyspark.sql.functions import corr
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("Data Analysis").getOrCreate()

def analyze_and_save_data(df, hdfs_path):
    # Calculate Correlation Matrix
    numeric_features = [t[0] for t in df.dtypes if t[1] == 'int' or t[1] == 'double']
    corr_matrix = df.select(numeric_features).toPandas().corr()

    # Save Correlation Matrix to HDFS
    spark.createDataFrame(corr_matrix).write.format("parquet").save(os.path.join(hdfs_path, "correlation_matrix.parquet"))

    # Generate and Save Distribution Plots
    for column in numeric_features:
        plt.figure(figsize=(10, 6))
        sns.distplot(df.select(column).toPandas())
        plt.title(f'Distribution of {column}')
        plt.savefig(f'/tmp/{column}.png')
        # Save plot to HDFS - assuming HDFS is mounted and accessible like a file system
        os.system(f"hdfs dfs -put /tmp/{column}.png {hdfs_path}/{column}.png")

    print("Analysis complete. Results saved to HDFS.")

# Example usage
df = spark.read.load("your_data_source")
analyze_and_save_data(df, "hdfs://HDP3PROD/user/miller6/test")

# Close the Spark session
spark.stop()
