from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("ETL") \
    .config('spark.driver.extraClassPath','/mnt/d/DE/projects/spotify/spotify-03/jars/postgresql-42.5.4.jar') \
    .getOrCreate()

def extract_movies_df():
    movies_df = spark.read \
        .format("jdbc") \
        .option("url","jdbc:postgresql://localhost:5432/etl") \
        .option("dbtable","movies") \
        .option("user","test_user1") \
        .option("password","12345") \
        .option("driver","org.postgresql.Driver") \
        .load()
    return movies_df



def extract_users_df():
    users_df = spark.read \
        .format("jdbc") \
        .option("url","jdbc:postgresql://localhost:5432/etl") \
        .option("dbtable","users") \
        .option("user","test_user1") \
        .option("password","12345") \
        .option("driver","org.postgresql.Driver") \
        .load()
    return users_df


# Transforming Data
def transform_avg_ratings(movies_df,users_df):
    avg_rating = users_df.groupBy("movie_id")\
                .mean("rating")\
                .withColumnRenamed("avg(rating)", "avg_rating")
    
    df = movies_df.join(avg_rating,movies_df.id == avg_rating.movie_id)
    
    df = df.drop("movie_id")
    return df

def load_df_to_db(df):
    mode = "overwrite"
    url = "jdbc:postgresql://localhost:5432/etl"
    properties = {
        "user" : "test_user1",
        "password" : "12345",
        "driver" : "org.postgresql.Driver"
    }
    df.write.jdbc(url = url,
                  table = "avg_ratings",
                  mode = mode,
                  properties = properties)
if __name__ == "__main__":
    movies_df = extract_movies_df()
    print("movies_df : ")
    movies_df.show()
    users_df = extract_users_df()
    print("users_df : ")
    users_df.show()
    df = transform_avg_ratings(movies_df,users_df)
    print("avg_ratings_df : ")
    df.show()
    load_df_to_db(df)
