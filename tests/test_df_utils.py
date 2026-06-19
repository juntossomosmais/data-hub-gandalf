from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp
from pyspark.sql.functions import when as sql_when
from pyspark.sql.types import StringType, StructField, StructType

from gandalf.utils.df_utils import generate_check_sum, generate_dim_sk, generate_fact_sk, hash_columns
from tests.resources.utils import PySparkTestCase


class TestHashColumns(PySparkTestCase):
    def setUp(self):
        # Arrange
        schema = StructType(
            [
                StructField("a", StringType()),
                StructField("b", StringType()),
            ]
        )
        self.df = self.spark.createDataFrame([("foo", "bar"), ("baz", None)], schema=schema)

    def test_returns_64_char_sha256_hex(self):
        # Act
        result = self.df.withColumn("h", hash_columns(col("a"))).first()["h"]

        # Assert
        self.assertEqual(len(result), 64)
        self.assertRegex(result, r"^[0-9a-f]{64}$")

    def test_null_replaced_with_asterisk(self):
        # Arrange
        schema = StructType([StructField("v", StringType())])
        df_null = self.spark.createDataFrame([(None,)], schema=schema)
        df_star = self.spark.createDataFrame([("*",)], schema=schema)

        # Act
        h_null = df_null.withColumn("h", hash_columns(col("v"))).first()["h"]
        h_star = df_star.withColumn("h", hash_columns(col("v"))).first()["h"]

        # Assert
        self.assertEqual(h_null, h_star)

    def test_empty_string_replaced_with_asterisk(self):
        # Arrange
        schema = StructType([StructField("v", StringType())])
        df_empty = self.spark.createDataFrame([("",)], schema=schema)
        df_star = self.spark.createDataFrame([("*",)], schema=schema)

        # Act
        h_empty = df_empty.withColumn("h", hash_columns(col("v"))).first()["h"]
        h_star = df_star.withColumn("h", hash_columns(col("v"))).first()["h"]

        # Assert
        self.assertEqual(h_empty, h_star)

    def test_different_values_produce_different_hashes(self):
        # Act
        h1 = self.df.filter("a = 'foo'").withColumn("h", hash_columns(col("a"))).first()["h"]
        h2 = self.df.filter("a = 'baz'").withColumn("h", hash_columns(col("a"))).first()["h"]

        # Assert
        self.assertNotEqual(h1, h2)

    def test_hash_is_deterministic(self):
        # Act
        h1 = self.df.withColumn("h", hash_columns(col("a"))).filter("a = 'foo'").first()["h"]
        h2 = self.df.withColumn("h", hash_columns(col("a"))).filter("a = 'foo'").first()["h"]

        # Assert
        self.assertEqual(h1, h2)

    def test_multi_column_hash(self):
        # Act
        result = self.df.withColumn("h", hash_columns(col("a"), col("b"))).filter("a = 'foo'").first()["h"]

        # Assert
        self.assertEqual(len(result), 64)

    def test_separator_prevents_collisions(self):
        # Arrange
        schema = StructType([StructField("x", StringType()), StructField("y", StringType())])
        df1 = self.spark.createDataFrame([("a", "bc")], schema=schema)
        df2 = self.spark.createDataFrame([("ab", "c")], schema=schema)

        # Act
        h1 = df1.withColumn("h", hash_columns(col("x"), col("y"))).first()["h"]
        h2 = df2.withColumn("h", hash_columns(col("x"), col("y"))).first()["h"]

        # Assert
        self.assertNotEqual(h1, h2)

    def test_no_separator_produces_collision(self):
        """Documents the known collision behaviour of the legacy hash_udf (separator='')."""
        # Arrange
        schema = StructType([StructField("x", StringType()), StructField("y", StringType())])
        df1 = self.spark.createDataFrame([("a", "bc")], schema=schema)
        df2 = self.spark.createDataFrame([("ab", "c")], schema=schema)

        # Act
        h1 = df1.withColumn("h", hash_columns(col("x"), col("y"), separator="")).first()["h"]
        h2 = df2.withColumn("h", hash_columns(col("x"), col("y"), separator="")).first()["h"]

        # Assert
        self.assertEqual(h1, h2)

    def test_usable_inside_when_otherwise(self):
        # Act
        result = self.df.withColumn(
            "h",
            sql_when(col("b").isNotNull(), col("b")).otherwise(hash_columns(col("a"))),
        )
        row_with_null_b = result.filter("a = 'baz'").first()

        # Assert
        self.assertEqual(len(row_with_null_b["h"]), 64)


class TestGenerateCheckSum(PySparkTestCase):
    def setUp(self):
        # Arrange
        self.schema = StructType(
            [
                StructField("id", StringType()),
                StructField("nome", StringType()),
            ]
        )
        self.df = self.spark.createDataFrame([("1", "Alice"), ("2", "Bob")], schema=self.schema)

    def test_adds_check_sum_column(self):
        # Act
        result = generate_check_sum(self.df, [])

        # Assert
        self.assertIn("scd_checksum", result.columns)

    def test_check_sum_is_sha256_hex(self):
        # Act
        value = generate_check_sum(self.df, []).filter("id = '1'").first()["scd_checksum"]

        # Assert
        self.assertEqual(len(value), 64)
        self.assertRegex(value, r"^[0-9a-f]{64}$")

    def test_null_replaced_with_asterisk_produces_same_hash_as_literal_asterisk(self):
        # Arrange
        df_null = self.spark.createDataFrame([("1", None)], schema=self.schema)
        df_star = self.spark.createDataFrame([("1", "*")], schema=self.schema)

        # Act
        hash_null = generate_check_sum(df_null, []).first()["scd_checksum"]
        hash_star = generate_check_sum(df_star, []).first()["scd_checksum"]

        # Assert
        self.assertEqual(hash_null, hash_star)

    def test_scd_control_cols_excluded_from_hash(self):
        # Arrange
        df_with_scd = (
            self.df.withColumn("scd_start_dt", current_timestamp())
            .withColumn("scd_end_dt", to_timestamp(lit("9999-12-31 00:00:00")))
            .withColumn("scd_checksum", lit("old_hash"))
            .withColumn("scd_is_current", lit(1))
        )

        # Act
        base_hash = generate_check_sum(self.df, []).filter("id = '1'").first()["scd_checksum"]
        scd_hash = generate_check_sum(df_with_scd, []).filter("id = '1'").first()["scd_checksum"]

        # Assert
        self.assertEqual(base_hash, scd_hash)

    def test_sk_columns_excluded_from_hash(self):
        # Arrange
        df_with_sk = self.df.withColumn("sk_client", lit("qualquer_sk"))

        # Act
        base_hash = generate_check_sum(self.df, []).filter("id = '1'").first()["scd_checksum"]
        sk_hash = generate_check_sum(df_with_sk, []).filter("id = '1'").first()["scd_checksum"]

        # Assert
        self.assertEqual(base_hash, sk_hash)

    def test_control_cols_param_excluded_from_hash(self):
        # Arrange
        df_with_meta = self.df.withColumn("created_at", lit("2024-01-01")).withColumn("updated_at", lit("2024-06-01"))

        # Act
        base_hash = generate_check_sum(self.df, []).filter("id = '1'").first()["scd_checksum"]
        meta_hash = (
            generate_check_sum(df_with_meta, ["created_at", "updated_at"]).filter("id = '1'").first()["scd_checksum"]
        )

        # Assert
        self.assertEqual(base_hash, meta_hash)

    def test_hash_is_deterministic(self):
        # Act
        h1 = generate_check_sum(self.df, []).filter("id = '1'").first()["scd_checksum"]
        h2 = generate_check_sum(self.df, []).filter("id = '1'").first()["scd_checksum"]

        # Assert
        self.assertEqual(h1, h2)

    def test_column_order_does_not_affect_hash(self):
        # Arrange
        df_reordered = self.df.select("nome", "id")

        # Act
        h_original = generate_check_sum(self.df, []).filter("id = '1'").first()["scd_checksum"]
        h_reordered = generate_check_sum(df_reordered, []).filter("id = '1'").first()["scd_checksum"]

        # Assert
        self.assertEqual(h_original, h_reordered)

    def test_different_values_produce_different_hashes(self):
        # Act
        h_alice = generate_check_sum(self.df, []).filter("id = '1'").first()["scd_checksum"]
        h_bob = generate_check_sum(self.df, []).filter("id = '2'").first()["scd_checksum"]

        # Assert
        self.assertNotEqual(h_alice, h_bob)

    def test_tracked_columns_hashes_only_listed_columns(self):
        # Arrange — same id, different nome; only id is tracked
        df_a = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)
        df_b = self.spark.createDataFrame([("1", "Bob")], schema=self.schema)

        # Act
        h_a = generate_check_sum(df_a, [], tracked_columns=["id"]).first()["scd_checksum"]
        h_b = generate_check_sum(df_b, [], tracked_columns=["id"]).first()["scd_checksum"]

        # Assert — nome is not tracked, so the checksum is identical
        self.assertEqual(h_a, h_b)

    def test_tracked_columns_missing_from_dataframe_raises(self):
        # Act / Assert
        with self.assertRaises(ValueError):
            generate_check_sum(self.df, [], tracked_columns=["does_not_exist"])


class TestGenerateDimSk(PySparkTestCase):
    def setUp(self):
        # Arrange
        schema = StructType(
            [
                StructField("id_client", StringType()),
                StructField("nome", StringType()),
            ]
        )
        df = self.spark.createDataFrame([("1", "Alice"), ("2", "Bob")], schema=schema)
        self.df = generate_check_sum(df, [])

    def test_adds_sk_column(self):
        # Act
        result = generate_dim_sk(self.df, ["id_client"], "sk_client")

        # Assert
        self.assertIn("sk_client", result.columns)

    def test_sk_is_64_char_hex(self):
        # Act
        sk = generate_dim_sk(self.df, ["id_client"], "sk_client").filter("id_client = '1'").first()["sk_client"]

        # Assert
        self.assertEqual(len(sk), 64)
        self.assertRegex(sk, r"^[0-9a-f]{64}$")

    def test_different_rows_produce_different_sks(self):
        # Act
        result = generate_dim_sk(self.df, ["id_client"], "sk_client")
        sk1 = result.filter("id_client = '1'").first()["sk_client"]
        sk2 = result.filter("id_client = '2'").first()["sk_client"]

        # Assert
        self.assertNotEqual(sk1, sk2)

    def test_requires_check_sum_column(self):
        # Arrange
        schema = StructType([StructField("id_client", StringType())])
        df_sem_checksum = self.spark.createDataFrame([("1",)], schema=schema)

        # Act / Assert
        with self.assertRaises(Exception):
            generate_dim_sk(df_sem_checksum, ["id_client"], "sk_client").collect()


class TestGenerateFactSk(PySparkTestCase):
    def setUp(self):
        # Arrange
        self.schema = StructType(
            [
                StructField("id_order", StringType()),
                StructField("id_client", StringType()),
            ]
        )
        self.df = self.spark.createDataFrame([("v1", "p1"), ("v2", "p2")], schema=self.schema)

    def test_adds_sk_column(self):
        # Act
        result = generate_fact_sk(self.df, ["id_order"], "sk_order")

        # Assert
        self.assertIn("sk_order", result.columns)

    def test_sk_is_string_type(self):
        # Act
        result = generate_fact_sk(self.df, ["id_order"], "sk_order")
        sk_field = next(f for f in result.schema.fields if f.name == "sk_order")

        # Assert
        self.assertIsInstance(sk_field.dataType, StringType)

    def test_sk_is_deterministic(self):
        # Act
        sk1 = generate_fact_sk(self.df, ["id_order"], "sk_order").filter("id_order = 'v1'").first()["sk_order"]
        sk2 = generate_fact_sk(self.df, ["id_order"], "sk_order").filter("id_order = 'v1'").first()["sk_order"]

        # Assert
        self.assertEqual(sk1, sk2)

    def test_different_rows_produce_different_sks(self):
        # Act
        result = generate_fact_sk(self.df, ["id_order"], "sk_order")
        sk1 = result.filter("id_order = 'v1'").first()["sk_order"]
        sk2 = result.filter("id_order = 'v2'").first()["sk_order"]

        # Assert
        self.assertNotEqual(sk1, sk2)

    def test_multi_column_ids(self):
        # Act
        sk = (
            generate_fact_sk(self.df, ["id_order", "id_client"], "sk_order")
            .filter("id_order = 'v1'")
            .first()["sk_order"]
        )

        # Assert
        self.assertIsNotNone(sk)
        self.assertRegex(sk, r"^-?\d+$")
