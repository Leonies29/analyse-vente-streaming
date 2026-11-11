"""Lot B — Schéma Spark des messages (doit suivre config/message_format.md)."""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

TRANSACTION_SCHEMA = StructType([
    StructField("InvoiceNo", StringType()),
    StructField("StockCode", StringType()),
    StructField("Description", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("InvoiceDate", StringType()),
    StructField("UnitPrice", DoubleType()),
    StructField("CustomerID", StringType()),
    StructField("Country", StringType()),
])
