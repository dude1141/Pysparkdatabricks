from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataMasker:
    """
    A production-ready data masking utility for PySpark DataFrames.
    Supports masking Aadhaar numbers and email addresses.
    """

    def __init__(self, spark_session):
        self.spark = spark_session

    # ----------------------------------------------------
    # Aadhaar Masking
    # ----------------------------------------------------
    def mask_aadhaar(self, df: DataFrame, column: str, output_column: str = "aadhar_masked") -> DataFrame:
        """
        Masks an Aadhaar number by showing only the last 4 digits.
        Works on Spark 3.2+.

        :param df: Input DataFrame
        :param column: Column containing Aadhaar number
        :param output_column: Name of masked Aadhaar column
        """
        return (
            df
            .withColumn(
                "aadhar_clean",
                F.regexp_replace(F.col(column), r"[^0-9]", "")
            )
            .withColumn(
                output_column,
                F.expr("""
                    concat(
                        repeat('*', greatest(length(aadhar_clean) - 4, 0)),
                        substring(aadhar_clean, greatest(length(aadhar_clean) - 3, 1), 4)
                    )
                """)
            )
            .drop("aadhar_clean")
        )

    # ----------------------------------------------------
    # Email Masking
    # ----------------------------------------------------
    def mask_email(self, df: DataFrame, column: str, output_column: str = "email_masked") -> DataFrame:
        """
        Masks an email address by keeping the first two characters
        of the local part and masking the remaining characters.

        :param df: Input DataFrame
        :param column: Column containing email
        :param output_column: Name of masked email column
        """
        df = (
            df
            .withColumn(
                "email_local",
                F.when(
                    F.col(column).isNotNull() & F.col(column).contains("@"),
                    F.split(F.col(column), "@").getItem(0)
                )
            )
            .withColumn(
                "email_domain",
                F.when(
                    F.col(column).isNotNull() & F.col(column).contains("@"),
                    F.split(F.col(column), "@").getItem(1)
                )
            )
        )

        df = (
            df
            .withColumn(
                output_column,
                F.when(
                    (F.col("email_local").isNotNull()) & (F.length("email_local") > 2),
                    F.concat(
                        F.substring(F.col("email_local"), 1, 2),
                        F.expr("repeat('*', length(email_local) - 2)"),
                        F.lit("@"),
                        F.col("email_domain")
                    )
                ).otherwise(F.col(column))
            )
            .drop("email_local", "email_domain")
        )

        return df

    # ----------------------------------------------------
    # Run all masking
    # ----------------------------------------------------
    def mask_all(self, df: DataFrame, aadhaar_col: str, email_col: str) -> DataFrame:
        """
        Applies Aadhaar masking + email masking on the DataFrame.

        :param df: Input DataFrame
        :param aadhaar_col: Aadhaar column name
        :param email_col: Email column name
        """
        df = self.mask_aadhaar(df, aadhaar_col, "aadhar_masked")
        df = self.mask_email(df, email_col, "email_masked")
        return df



