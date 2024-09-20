from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round
from pyspark.sql.functions import to_date, when, min
from pyspark.sql import DataFrame
import logging


standard = "./raw/AB_NYC_2019.csv"
raw = "./raw"
processed = "./processed"
snaps = "./snapshots"


# Provide logging
logging.basicConfig(filename='./logs/etl.log', level=logging.INFO, format='%(asctime)s %(message)s')

spark = SparkSession.builder.appName("MicroBatchEtl").getOrCreate()

# Setting up the schema for the input CSV files
schema = spark.read \
            .option("inferSchema", "true")\
            .option("header", "true")\
            .csv(standard)\
            .limit(3)\
            .schema

# Reading from the raw directory with Structured Streaming
stream = spark.readStream \
            .option("inferSchema", "true")\
            .option("header", "true")\
            .schema(schema) \
            .csv(raw)

def clean_and_transform_data(df,default_date="2000-01-01"):
    """
    Clean and transform data into valid dataframe
    
    :param df: PySpark DataFrame to be provided
    :default_date: Date which NA values in last_review column
    """

    # Data Cleaning  
    # Filter out rows where price is 0 or negative
    cleaned_df = df.filter(df.price > 0)

    # Convert the last_review column to a proper date format
    cleaned_df = cleaned_df.withColumn("last_review", to_date("last_review", "yyyy-MM-dd"))

    # Fill missing last_review values with the earliest date 
    cleaned_df = cleaned_df.fillna({"last_review": default_date})

    # Fill missing reviews_per_month values with 0
    cleaned_df = cleaned_df.fillna({"reviews_per_month": 0})

    # Drop rows with missing latitude or longitude
    cleaned_df = cleaned_df.na.drop(subset=["latitude", "longitude"])

    # Data Transformation
    # Categorize listings into price ranges (budget, mid-range, luxury)
    transformed_df = cleaned_df.withColumn("price_category", 
        when(cleaned_df.price < 50, "budget")
        .when(cleaned_df.price.between(50, 150), "mid-range")
        .otherwise("luxury"))

    # Create a column to calculate the average price_per_review
    transformed_df = transformed_df.withColumn(
        "price_per_review", 
        round(cleaned_df.price / cleaned_df.reviews_per_month, 2)
    )
    
    # Fill missing price_per_review values with 0
    transformed_df = transformed_df.fillna({"price_per_review": 0})

    return transformed_df

def register_temp_view(df, view_name = "listings"):
    """
    Register a DataFrame as a temporary SQL table.
    
    :param df: PySpark DataFrame to be registered
    :param view_name: Name of the temporary SQL view, default - "listings"
    """
    df.createOrReplaceTempView(view_name)

def get_listings_by_neighborhood_group(df, table="listings"):
    """
    Get the number of listings per neighborhood group, 
    sorted by the number of listings in DESC.
    
    :param df: Dataframe batch 
    :param table: Table on which query is performed, default - listings
    :return: DataFrame with neighborhood group and listing count
    """
    query = f"""
    SELECT neighbourhood_group, COUNT(*) AS listing_count
    FROM {table}
    GROUP BY neighbourhood_group
    ORDER BY listing_count DESC
    """
    return df.sparkSession.sql(query)

def get_top_10_most_expensive_listings(df, table="listings"):
    """
    Get the top 10 most expensive listings.
    
    :param df: Dataframe batch 
    :param table: Table on which query is performed, default - listings
    :return: DataFrame with the top 10 most expensive listings
    """
    #Note sorting without aggregating in streaming doesn't work
    #also window functions like row_number can't be used here
    #evenmore i can't use subqueries or cte's 
    # it can be done in static data, not streaming
    #so made a decission to use following query
    # spark.sql("""
    #     #     
    #     # """)
    query = f"""
    SELECT *
    FROM {table}
    ORDER BY price DESC
    LIMIT 10
    """
    return df.sparkSession.sql(query)

def get_average_price_by_room_type(df, table="listings"):
    """
    Get the average price per room type, grouped by neighborhood group.
    
    :param df: Dataframe batch 
    :param table: Table on which query is performed, default - listings
    :return: DataFrame with average prices
    """
    query = f"""
    SELECT neighbourhood_group, room_type, AVG(price) AS avg_price
    FROM {table}
    GROUP BY neighbourhood_group, room_type
    ORDER BY neighbourhood_group, room_type
    """
    return df.sparkSession.sql(query)

def repartition_and_save(df):
    """
    Repartition the DataFrame by 'neighbourhood_group'
    and Save it as parquet files
    
    :param df: Dataframe on which operations are occuring
    """
    # Repartition the data by neighbourhood_group and save as parquet file
    df.repartition("neighbourhood_group") \
        .write \
        .partitionBy("neighbourhood_group") \
        .mode('append') \
        .parquet(processed)

# Data Quality Checks
def data_quality_validation(df, actual_count):

    # Row Count Validation
    row_count = df.count()
    logging.info(f"Row count actual: {row_count}, expected:{actual_count}")
    if row_count != actual_count:
        logging.error("Actual and expected row count don't match")

    critical_columns = ["price", "minimum_nights", "availability_365"]
    for column in critical_columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count != 0:
            logging.error(
                # THERE WILL BE NULL VALUES IN AVAILABILITY_365, BECAUSE TASK DIDN'T REQUIRE TO HANDLE SUCH CASE
                f"NULL values found in column '{column}': {null_count} records"
            )
        else:
            logging.info(f"No NULL values in column '{column}'")
            
    logging.info("Data quality validation is succesful")


def process_batch(df, batch_id):

    try:
        logging.info(f"Starting batch {batch_id}")
        # take count of rows 
        actual_count = df.count()

        #cleaning and transforming data
        transformed = clean_and_transform_data(df)

        #register temporary table and perform sql queries
        register_temp_view(transformed)

        #perform sql queries 
        neighbours_listings = get_listings_by_neighborhood_group(df)
        neighbours_listings.show()

        ten_exp_listings = get_top_10_most_expensive_listings(df)
        ten_exp_listings.show()

        avg_price_by_room = get_average_price_by_room_type(df)
        avg_price_by_room.show()
        
        #save data partitioned by neighbourhood into parquet files
        repartition_and_save(transformed)

        #validate outcome data
        data_quality_validation(transformed, actual_count)

        logging.info(f"Batch ID {batch_id} processed successfully")

    except Exception as e:
        logging.error(f"Error processing batch: {e}")


query = stream.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", snaps)\
    .start()

query.awaitTermination()