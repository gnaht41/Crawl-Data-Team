from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace

# Khởi tạo phiên Spark với MongoDB và PostgreSQL
spark = SparkSession.builder \
    .appName("Goodreads Spark with MongoDB and PostgreSQL") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.7.4") \
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/goodreads_db.books") \
    .getOrCreate()

# Cài đặt mức độ log
spark.sparkContext.setLogLevel("INFO")

# Đọc dữ liệu từ MongoDB vào DataFrame
df = spark.read \
    .format("mongo") \
    .option("uri", "mongodb://localhost:27017/goodreads_db.books") \
    .load()

# Hiển thị dữ liệu từ MongoDB ban đầu
df.show(5)
df.printSchema()

# Kiểm tra dữ liệu trước khi chuyển đổi
df.select("Number of Ratings", "Reviews").show(5)

# Bước 1: Xử lý dữ liệu có dạng "8,932,568" - Loại bỏ dấu phẩy
df = df.withColumn("Number of Ratings", regexp_replace(col("Number of Ratings"), ",", "")) \
       .withColumn("Reviews", regexp_replace(col("Reviews"), ",", ""))

# Bước 2: Chuyển đổi kiểu dữ liệu sau khi loại bỏ dấu phẩy
df = df.withColumn("Pages", col("Pages").cast("int")) \
       .withColumn("Rating", col("Rating").cast("float")) \
       .withColumn("Number of Ratings", col("Number of Ratings").cast("int")) \
       .withColumn("Reviews", col("Reviews").cast("int"))

# Hiển thị dữ liệu sau khi chuyển đổi
df.select("Number of Ratings", "Reviews").show(5)

# Bước 3: Xử lý dữ liệu null (nếu cần)
df = df.na.fill({
    "Pages": 0,
    "Rating": 0.0,
    "Number of Ratings": 0,
    "Reviews": 0
})

# Tạo các bảng từ dữ liệu
authors_df = df.select("Author").distinct(
).withColumnRenamed("Author", "author_name")

books_df = df.select("Title", "Author", "Pages", "Cover Type", "Date") \
             .withColumnRenamed("Title", "book_title") \
             .withColumnRenamed("Author", "author_name") \
             .withColumnRenamed("Pages", "num_pages") \
             .withColumnRenamed("Cover Type", "cover_type") \
             .withColumnRenamed("Date", "publish_date")

ratings_df = df.select("Title", "Rating", "Number of Ratings", "Reviews") \
               .withColumnRenamed("Title", "book_title") \
               .withColumnRenamed("Rating", "rating") \
               .withColumnRenamed("Number of Ratings", "num_ratings") \
               .withColumnRenamed("Reviews", "num_reviews")

# Kết nối tới PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/goodreads_books"
connection_properties = {
    "user": "postgres",
    "password": "thangvt4102004",
    "driver": "org.postgresql.Driver"
}


def write_to_postgres(df, table_name):
    try:
        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="overwrite",
            properties=connection_properties
        )
        print(f"Đã ghi thành công bảng {table_name} vào PostgreSQL!")
    except Exception as e:
        print(f"Lỗi khi ghi bảng {table_name} vào PostgreSQL: {e}")


# Lưu bảng tác giả, sách và đánh giá vào PostgreSQL
write_to_postgres(authors_df, "authors")
write_to_postgres(books_df, "books")
write_to_postgres(ratings_df, "ratings")
