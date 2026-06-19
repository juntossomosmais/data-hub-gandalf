from uuid import uuid4

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from gandalf.exceptions import DuplicatedDataFrameError, InvalidColumnsError, InvalidTable, RowCountException
from gandalf.utils.df_validations import check_duplicates, column_check, row_count, table_check
from tests.resources.utils import PySparkTestCase


class TestCheckDuplicates(PySparkTestCase):
    """Testes para check_duplicates — sem Delta, apenas DataFrames em memória."""

    def _schema(self):
        return StructType(
            [
                StructField("id", StringType()),
                StructField("nome", StringType()),
                StructField("scd_is_current", IntegerType()),
            ]
        )

    def test_sem_duplicatas_nao_levanta_excecao(self):
        # Arrange
        df = self.spark.createDataFrame([("1", "Alice", 1), ("2", "Bob", 1)], schema=self._schema())

        # Act / Assert
        check_duplicates(df, ["id"], False)

    def test_com_duplicatas_levanta_duplicated_error(self):
        # Arrange
        df = self.spark.createDataFrame([("1", "Alice", 1), ("1", "Alice Dup", 1)], schema=self._schema())

        # Act / Assert
        with self.assertRaises(DuplicatedDataFrameError):
            check_duplicates(df, ["id"], False)

    def test_current_flag_true_ignora_historico(self):
        # Arrange
        # versão antiga (flag=0) e versão atual (flag=1) do mesmo id — não deve falhar
        df = self.spark.createDataFrame([("1", "Alice v1", 0), ("1", "Alice v2", 1)], schema=self._schema())

        # Act / Assert
        check_duplicates(df, ["id"], True)

    def test_current_flag_false_ve_todas_as_linhas(self):
        # Arrange
        # sem filtro de flag, duas linhas com mesmo id são duplicatas
        df = self.spark.createDataFrame([("1", "Alice v1", 0), ("1", "Alice v2", 1)], schema=self._schema())

        # Act / Assert
        with self.assertRaises(DuplicatedDataFrameError):
            check_duplicates(df, ["id"], False)


class TestTableCheck(PySparkTestCase):
    """Testes para table_check — verifica existência de tabelas Delta."""

    def setUp(self):
        self.path = self.unique_storage_path()

    def test_path_delta_valido_nao_levanta_excecao(self):
        # Arrange
        schema = StructType([StructField("id", StringType())])
        self.spark.createDataFrame([("1",)], schema=schema).write.format("delta").mode("overwrite").save(self.path)

        # Act / Assert
        table_check(self.spark, self.path, "storage")

    def test_path_inexistente_levanta_invalid_table(self):
        # Arrange
        missing_path = f"{self.unique_storage_path()}/nao_existe"

        # Act / Assert
        with self.assertRaises(InvalidTable):
            table_check(self.spark, missing_path, "storage")

    def test_path_nao_delta_levanta_invalid_table(self):
        # Arrange
        # escreve parquet (não delta) — deve falhar
        schema = StructType([StructField("id", StringType())])
        self.spark.createDataFrame([("1",)], schema=schema).write.mode("overwrite").parquet(self.path)

        # Act / Assert
        with self.assertRaises(InvalidTable):
            table_check(self.spark, self.path, "storage")

    def test_nome_tabela_inexistente_levanta_invalid_table(self):
        # Arrange
        # DeltaTable.forName is lazy over Spark Connect (it does not eagerly validate that the
        # table exists), so a missing named table is only detectable on classic Spark / Databricks.
        if "connect" in type(self.spark).__module__:
            self.skipTest("DeltaTable.forName is lazy over Spark Connect")

        # Act / Assert
        with self.assertRaises(InvalidTable):
            table_check(self.spark, f"spark_catalog.gandalf_it.missing_{uuid4().hex}", "table")


class TestColumnCheck(PySparkTestCase):
    """Testes para column_check — valida compatibilidade de schema entre source e target."""

    def setUp(self):
        self.path = self.unique_storage_path()

    def _write_target(self, spark, schema, data=None):
        df = spark.createDataFrame([], schema) if data is None else spark.createDataFrame(data, schema=schema)
        df.write.format("delta").mode("overwrite").save(self.path)

    def test_schemas_identicos_nao_levanta_excecao(self):
        # Arrange
        schema = StructType([StructField("id", StringType()), StructField("nome", StringType())])
        self._write_target(self.spark, schema)
        df_source = self.spark.createDataFrame([("1", "Alice")], schema=schema)

        # Act / Assert
        column_check(self.spark, df_source, self.path, "storage")

    def test_coluna_extra_no_source_levanta_invalid_columns(self):
        # Arrange
        schema_target = StructType([StructField("id", StringType())])
        self._write_target(self.spark, schema_target)
        schema_source = StructType([StructField("id", StringType()), StructField("extra", StringType())])
        df_source = self.spark.createDataFrame([("1", "extra")], schema=schema_source)

        # Act / Assert
        with self.assertRaises(InvalidColumnsError):
            column_check(self.spark, df_source, self.path, "storage")

    def test_coluna_faltando_no_source_levanta_invalid_columns(self):
        # Arrange
        schema_target = StructType([StructField("id", StringType()), StructField("obrigatoria", StringType())])
        self._write_target(self.spark, schema_target)
        schema_source = StructType([StructField("id", StringType())])
        df_source = self.spark.createDataFrame([("1",)], schema=schema_source)

        # Act / Assert
        with self.assertRaises(InvalidColumnsError):
            column_check(self.spark, df_source, self.path, "storage")

    def test_colunas_scd_no_target_sao_ignoradas(self):
        # Arrange
        # target tem colunas SCD que source não precisa ter
        schema_target = StructType(
            [
                StructField("id", StringType()),
                StructField("nome", StringType()),
                StructField("scd_start_dt", StringType()),
                StructField("scd_end_dt", StringType()),
                StructField("scd_checksum", StringType()),
                StructField("scd_is_current", IntegerType()),
            ]
        )
        self._write_target(self.spark, schema_target)
        schema_source = StructType([StructField("id", StringType()), StructField("nome", StringType())])
        df_source = self.spark.createDataFrame([("1", "Alice")], schema=schema_source)

        # Act / Assert
        column_check(self.spark, df_source, self.path, "storage")

    def test_colunas_sk_no_target_sao_ignoradas(self):
        # Arrange
        schema_target = StructType(
            [
                StructField("id", StringType()),
                StructField("nome", StringType()),
                StructField("sk_entidade", StringType()),
            ]
        )
        self._write_target(self.spark, schema_target)
        schema_source = StructType([StructField("id", StringType()), StructField("nome", StringType())])
        df_source = self.spark.createDataFrame([("1", "Alice")], schema=schema_source)

        # Act / Assert
        column_check(self.spark, df_source, self.path, "storage")


class TestRowCount(PySparkTestCase):
    """Testes para row_count — valida contagem de linhas ativas após merge."""

    def _target_df(self, spark, rows):
        schema = StructType([StructField("id", StringType()), StructField("scd_is_current", IntegerType())])
        return spark.createDataFrame(rows, schema=schema)

    def test_contagens_iguais_nao_levanta_excecao(self):
        # Arrange
        df_target = self._target_df(self.spark, [("1", 1), ("2", 1)])

        # Act / Assert
        row_count(2, df_target)

    def test_contagem_diferente_levanta_row_count_exception(self):
        # Arrange
        df_target = self._target_df(self.spark, [("1", 1)])

        # Act / Assert
        with self.assertRaises(RowCountException):
            row_count(3, df_target)

    def test_filtra_apenas_current_flag_1(self):
        # Arrange
        # target tem 3 linhas mas apenas 2 com scd_is_current=1
        df_target = self._target_df(self.spark, [("1", 1), ("2", 1), ("1", 0)])

        # Act / Assert
        row_count(2, df_target)
