from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Khởi tạo phiên Spark với driver PostgreSQL
spark = SparkSession.builder \
    .appName("Goodreads Spark with PostgreSQL") \
    .config("spark.jars", "file:///C:/Spark/postgresql-42.7.4.jar")\
    .getOrCreate()

# Đọc dữ liệu từ file CSV
file_path = "goodreads_books.csv"
df = spark.read.option("header", True).csv(file_path)

# Hiển thị dữ liệu để xem xét cấu trúc
df.show(5)
df.printSchema()

# Phân tích và tạo các bảng từ dữ liệu gốc
# Bảng Tác giả (Authors)
authors_df = df.select("Author").distinct(
).withColumnRenamed("Author", "author_name")

# Bảng Sách (Books)
books_df = df.select("Title", "Author", "Pages", "Cover Type", "Date") \
             .withColumnRenamed("Title", "book_title") \
             .withColumnRenamed("Author", "author_name") \
             .withColumnRenamed("Pages", "num_pages") \
             .withColumnRenamed("Cover Type", "cover_type") \
             .withColumnRenamed("Date", "publish_date")

# Bảng Đánh giá (Ratings)
ratings_df = df.select("Title", "Rating", "Number of Ratings", "Reviews") \
               .withColumnRenamed("Title", "book_title") \
               .withColumnRenamed("Rating", "rating") \
               .withColumnRenamed("Number of Ratings", "num_ratings") \
               .withColumnRenamed("Reviews", "num_reviews")

# Cấu hình kết nối tới PostgreSQL qua JDBC
jdbc_url = "jdbc:postgresql://localhost:5432/goodreads_books"
connection_properties = {
    "user": "postgres",
    "password": "thangvt4102004",
    "driver": "org.postgresql.Driver"
}

# Lưu bảng tác giả vào PostgreSQL
authors_df.write.jdbc(
    url=jdbc_url,
    table="authors",
    mode="overwrite",
    properties=connection_properties
)

# Lưu bảng sách vào PostgreSQL
books_df.write.jdbc(
    url=jdbc_url,
    table="books",
    mode="overwrite",
    properties=connection_properties
)

# Lưu bảng đánh giá vào PostgreSQL
ratings_df.write.jdbc(
    url=jdbc_url,
    table="ratings",
    mode="overwrite",
    properties=connection_properties
)
