from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def create_spark_session():
    """Create Spark session with necessary configurations"""
    return SparkSession.builder \
        .appName("Sales Analysis") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .master("local[*]") \
        .getOrCreate()

file_path = 'C:\\Users\\suran\\Downloads\\python_project\\spark-analysis-app\\sales_data.csv'
def read_sales_data(spark, file_path):
    """Read sales data from CSV file"""
    # Debug information
    print(f"Attempting to read file from: {file_path}")
    print(f"File exists: {os.path.exists(file_path)}")
    
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price_per_unit", DoubleType(), True),
        StructField("total_sales_amount", DoubleType(), True),
        StructField("sales_date", DateType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("discount", DoubleType(), True),
        StructField("shipping_cost", DoubleType(), True),
        StructField("payment_method", StringType(), True)
    ])
    
    # Use absolute file path with file:/// prefix
    absolute_path = os.path.abspath(file_path)
    spark_path = f"file:///{absolute_path.replace(os.sep, '/')}"
    print(f"Using Spark path: {spark_path}")
    
    return spark.read \
        .option("header", "true") \
        .option("mode", "DROPMALFORMED") \
        .schema(schema) \
        .csv(spark_path)

def analyze_sales(df, output_dir):
    """Perform various sales analyses"""
    
    # Create metrics directory if it doesn't exist
    metrics_dir = os.path.join(output_dir, "metrics")
    print(f"Creating metrics directory at: {metrics_dir}")
    if not os.path.exists(metrics_dir):
        os.makedirs(metrics_dir)
    
    # Show sample data and schema for debugging
    print("\nDataFrame Schema:")
    df.printSchema()
    print("\nSample Data:")
    df.show(5)
    
    # 1. Total Sales by State
    print("\nGenerating state sales analysis...")
    state_sales = df.groupBy("state") \
        .agg(
            round(sum("total_sales_amount"), 2).alias("total_sales"),
            count("*").alias("number_of_orders"),
            round(avg("total_sales_amount"), 2).alias("avg_order_value")
        ) \
        .orderBy(desc("total_sales"))
    state_sales_path = os.path.join(metrics_dir, "state_sales")
    state_sales.write.mode("overwrite").option("header", "true").csv(state_sales_path)

    # [Rest of the analysis functions remain the same...]
    # Only showing first analysis for brevity, but others would follow same pattern

def main():
    """Main function to execute the sales analysis"""
    # File paths
    current_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(current_dir, "sales_data.csv")
    
    print(f"Current working directory: {os.getcwd()}")
    print(f"Script directory: {current_dir}")
    print(f"Input file path: {input_file}")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read sales data
        print("\nReading sales data...")
        sales_df = read_sales_data(spark, input_file)
        
        # Perform analysis
        print("\nPerforming sales analysis...")
        analyze_sales(sales_df, current_dir)
        
        print("\nAnalysis complete! Results saved in 'metrics' directory.")
    
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        print("Stack trace:")
        import traceback
        traceback.print_exc()
    
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()
