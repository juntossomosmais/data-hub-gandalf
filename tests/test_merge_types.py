import unittest
from datetime import datetime

from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from gandalf import SCDColumns
from gandalf.exceptions import InvalidColumnsError
from gandalf.merge.merge_types import overwrite, overwrite_partition, scd2_batch, upsert_merge
from gandalf.utils.df_utils import generate_check_sum
from tests.resources.utils import PySparkTestCase


def _schema_basico():
    return StructType([StructField("id", StringType()), StructField("nome", StringType())])


def _schema_scd():
    """Schema completo para tabela SCD2 target (vazia).

    scd_start_dt e scd_end_dt são TimestampType: scd2_batch grava timestamps e um
    target StringType faria o write Delta falhar.
    """
    return StructType(
        [
            StructField("id", StringType()),
            StructField("nome", StringType()),
            StructField("scd_checksum", StringType()),
            StructField("scd_is_current", IntegerType()),
            StructField("scd_start_dt", TimestampType()),
            StructField("scd_end_dt", TimestampType()),
        ]
    )


def _escreve_tabela_scd_vazia(spark, path):
    spark.createDataFrame([], _schema_scd()).write.format("delta").mode("overwrite").save(path)


def _escreve_tabela_vazia(spark, path, schema):
    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(path)


class TestScd2Batch(PySparkTestCase):
    """Testes para scd2_batch — SCD Type 2 com e sem deleção."""

    def setUp(self):
        self.path = self.unique_storage_path()
        _escreve_tabela_scd_vazia(self.spark, self.path)
        self.schema = _schema_basico()

    def _source(self, data):
        return generate_check_sum(self.spark.createDataFrame(data, schema=self.schema), [])

    def test_carga_inicial_insere_todas_as_linhas(self):
        # Arrange
        df = self._source([("1", "Alice"), ("2", "Bob")])
        # Act
        scd2_batch(self.spark, df, ["id"], self.path, "storage")
        # Assert
        result = self.spark.read.format("delta").load(self.path)
        self.assertEqual(result.filter("scd_is_current = 1").count(), 2)
        self.assertEqual(result.filter("scd_end_dt = '9999-12-31 00:00:00'").count(), 2)

    def test_linha_sem_alteracao_nao_cria_nova_versao(self):
        # Arrange
        df = self._source([("1", "Alice")])
        # Act
        scd2_batch(self.spark, df, ["id"], self.path, "storage")
        scd2_batch(self.spark, df, ["id"], self.path, "storage")
        # Assert
        result = self.spark.read.format("delta").load(self.path)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result.filter("scd_is_current = 1").count(), 1)

    def test_linha_alterada_cria_nova_versao_scd2(self):
        # Arrange
        df_v1 = self._source([("1", "Alice")])
        scd2_batch(self.spark, df_v1, ["id"], self.path, "storage")
        df_v2 = self._source([("1", "Alice Atualizada")])
        # Act
        scd2_batch(self.spark, df_v2, ["id"], self.path, "storage")
        # Assert
        result = self.spark.read.format("delta").load(self.path)
        self.assertEqual(result.count(), 2)
        self.assertEqual(result.filter("scd_is_current = 1").count(), 1)
        self.assertEqual(result.filter("scd_is_current = 0").count(), 1)
        self.assertEqual(result.filter("scd_is_current = 1").first()["nome"], "Alice Atualizada")

    def test_linha_ausente_marcada_como_deletada(self):
        # Arrange
        df_completo = self._source([("1", "Alice"), ("2", "Bob")])
        scd2_batch(self.spark, df_completo, ["id"], self.path, "storage")
        df_parcial = self._source([("1", "Alice")])
        # Act
        scd2_batch(self.spark, df_parcial, ["id"], self.path, "storage")
        # Assert
        result = self.spark.read.format("delta").load(self.path)
        # Bob is absent from the second load -> SCD2 closes his current version (scd_is_current=0).
        # No 'deleted' tombstone row is written: the 'deleted' checksum is an internal merge
        # sentinel used to force the close-update, not data persisted to the table.
        bob = result.filter("id = '2'")
        self.assertEqual(bob.filter("scd_is_current = 1").count(), 0)
        self.assertEqual(bob.filter("scd_is_current = 0").count(), 1)

    def test_nova_linha_inserida_na_segunda_carga(self):
        # Arrange
        df_v1 = self._source([("1", "Alice")])
        scd2_batch(self.spark, df_v1, ["id"], self.path, "storage")
        df_v2 = self._source([("1", "Alice"), ("3", "Charlie")])
        # Act
        scd2_batch(self.spark, df_v2, ["id"], self.path, "storage")
        # Assert
        result = self.spark.read.format("delta").load(self.path)
        self.assertEqual(result.filter("scd_is_current = 1").count(), 2)
        self.assertIsNotNone(result.filter("id = '3' AND scd_is_current = 1").first())

    def test_ignore_delete_nao_marca_linhas_ausentes(self):
        # Arrange
        df_completo = self._source([("1", "Alice"), ("2", "Bob")])
        scd2_batch(self.spark, df_completo, ["id"], self.path, "storage", ignore_delete=True)
        df_parcial = self._source([("1", "Alice")])
        # Act
        scd2_batch(self.spark, df_parcial, ["id"], self.path, "storage", ignore_delete=True)
        # Assert
        result = self.spark.read.format("delta").load(self.path)
        bob = result.filter("id = '2' AND scd_is_current = 1").first()
        self.assertIsNotNone(bob)
        self.assertNotEqual(bob["scd_checksum"], "deleted")

    def test_colunas_de_controle_customizadas(self):
        # Arrange — target com nomes de controle totalmente customizados
        cols = SCDColumns(start_dt="dt_ini", end_dt="dt_fim", is_current="ativo", checksum="hash_n")
        path = self.unique_storage_path()
        custom_schema = StructType(
            [
                StructField("id", StringType()),
                StructField("nome", StringType()),
                StructField("hash_n", StringType()),
                StructField("ativo", IntegerType()),
                StructField("dt_ini", TimestampType()),
                StructField("dt_fim", TimestampType()),
            ]
        )
        self.spark.createDataFrame([], custom_schema).write.format("delta").mode("overwrite").save(path)

        def _src(data):
            return generate_check_sum(self.spark.createDataFrame(data, schema=self.schema), [], checksum_col="hash_n")

        # Act — carga inicial e alteração
        scd2_batch(self.spark, _src([("1", "Alice")]), ["id"], path, "storage", columns=cols)
        scd2_batch(self.spark, _src([("1", "Alice Atualizada")]), ["id"], path, "storage", columns=cols)

        # Assert
        result = self.spark.read.format("delta").load(path)
        self.assertEqual(result.count(), 2)
        self.assertEqual(result.filter("ativo = 1").count(), 1)
        self.assertEqual(result.filter("ativo = 1").first()["nome"], "Alice Atualizada")

    def test_target_existente_sem_colunas_de_controle_levanta_invalid_columns(self):
        # Arrange — target não-vazio sem as colunas de controle SCD
        path = self.unique_storage_path()
        self.spark.createDataFrame([("1", "Alice")], schema=self.schema).write.format("delta").mode("overwrite").save(
            path
        )

        # Act / Assert
        with self.assertRaises(InvalidColumnsError):
            scd2_batch(self.spark, self._source([("2", "Bob")]), ["id"], path, "storage")

    def test_source_com_coluna_merge_key_levanta_invalid_columns(self):
        # Arrange — uma coluna de source chamada "merge_key" colide com o staging interno
        schema = StructType(
            [
                StructField("id", StringType()),
                StructField("nome", StringType()),
                StructField("merge_key", StringType()),
            ]
        )
        df = generate_check_sum(self.spark.createDataFrame([("1", "Alice", "x")], schema=schema), [])

        # Act / Assert
        with self.assertRaises(InvalidColumnsError):
            scd2_batch(self.spark, df, ["id"], self.path, "storage")

    def test_target_checksum_nao_string_levanta_invalid_columns(self):
        # Arrange — target não-vazio com checksum tipado errado (Int em vez de String)
        path = self.unique_storage_path()
        schema = StructType(
            [
                StructField("id", StringType()),
                StructField("nome", StringType()),
                StructField("scd_checksum", IntegerType()),
                StructField("scd_is_current", IntegerType()),
                StructField("scd_start_dt", TimestampType()),
                StructField("scd_end_dt", TimestampType()),
            ]
        )
        self.spark.createDataFrame(
            [("1", "Alice", 0, 1, datetime(2024, 1, 1), datetime(2025, 1, 1))], schema=schema
        ).write.format("delta").mode("overwrite").save(path)

        # Act / Assert
        with self.assertRaises(InvalidColumnsError):
            scd2_batch(self.spark, self._source([("2", "Bob")]), ["id"], path, "storage")

    def test_target_flag_nao_integer_levanta_invalid_columns(self):
        # Arrange — target não-vazio com current-flag tipado errado (String em vez de Int)
        path = self.unique_storage_path()
        schema = StructType(
            [
                StructField("id", StringType()),
                StructField("nome", StringType()),
                StructField("scd_checksum", StringType()),
                StructField("scd_is_current", StringType()),
                StructField("scd_start_dt", TimestampType()),
                StructField("scd_end_dt", TimestampType()),
            ]
        )
        self.spark.createDataFrame(
            [("1", "Alice", "h", "1", datetime(2024, 1, 1), datetime(2025, 1, 1))], schema=schema
        ).write.format("delta").mode("overwrite").save(path)

        # Act / Assert
        with self.assertRaises(InvalidColumnsError):
            scd2_batch(self.spark, self._source([("2", "Bob")]), ["id"], path, "storage")

    def test_insert_qualifica_source_evita_ambiguous_reference(self):
        """whenNotMatchedInsert qualifica colunas de source; col(c) puro é AMBIGUOUS_REFERENCE no Spark 4.0."""
        # Arrange
        scd2_batch(self.spark, self._source([("1", "Alice")]), ["id"], self.path, "storage")

        # Act — "2" é chave nova, força a cláusula whenNotMatchedInsert
        scd2_batch(self.spark, self._source([("1", "Alice"), ("2", "Bob")]), ["id"], self.path, "storage")

        # Assert
        result = self.spark.read.format("delta").load(self.path)
        self.assertIsNotNone(result.filter("id = '2' AND scd_is_current = 1").first())


class TestUpsertMerge(PySparkTestCase):
    """Testes para upsert_merge — insert/update sem histórico SCD."""

    def setUp(self):
        self.path = self.unique_storage_path()
        self.schema = _schema_basico()
        _escreve_tabela_vazia(self.spark, self.path, self.schema)

    def test_carga_inicial_insere_todos_os_registros(self):
        # Arrange
        df = self.spark.createDataFrame([("1", "Alice"), ("2", "Bob")], schema=self.schema)
        # Act
        upsert_merge(self.spark, df, ["id"], self.path, "storage")
        # Assert
        self.assertEqual(self.spark.read.format("delta").load(self.path).count(), 2)

    def test_registro_existente_e_atualizado(self):
        # Arrange
        df_v1 = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)
        upsert_merge(self.spark, df_v1, ["id"], self.path, "storage")
        df_v2 = self.spark.createDataFrame([("1", "Alice Atualizada")], schema=self.schema)
        # Act
        upsert_merge(self.spark, df_v2, ["id"], self.path, "storage")
        # Assert
        result = self.spark.read.format("delta").load(self.path)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result.first()["nome"], "Alice Atualizada")

    def test_novo_registro_e_inserido(self):
        # Arrange
        df_v1 = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)
        upsert_merge(self.spark, df_v1, ["id"], self.path, "storage")
        df_v2 = self.spark.createDataFrame([("2", "Bob")], schema=self.schema)
        # Act
        upsert_merge(self.spark, df_v2, ["id"], self.path, "storage")
        # Assert
        self.assertEqual(self.spark.read.format("delta").load(self.path).count(), 2)

    def test_ignore_update_cols_preserva_valor_original(self):
        # Arrange
        schema = StructType(
            [
                StructField("id", StringType()),
                StructField("nome", StringType()),
                StructField("created_at", StringType()),
            ]
        )
        path = self.unique_storage_path()
        self.spark.createDataFrame([("1", "Alice", "2024-01-01")], schema=schema).write.format("delta").mode(
            "overwrite"
        ).save(path)
        df_update = self.spark.createDataFrame([("1", "Alice Atualizada", "2024-12-31")], schema=schema)
        # Act
        upsert_merge(self.spark, df_update, ["id"], path, "storage", ignore_update_cols=["created_at"])
        # Assert
        result = self.spark.read.format("delta").load(path).first()
        self.assertEqual(result["created_at"], "2024-01-01")
        self.assertEqual(result["nome"], "Alice Atualizada")

    def test_has_delete_remove_registros_ausentes(self):
        # Arrange
        df_v1 = self.spark.createDataFrame([("1", "Alice"), ("2", "Bob")], schema=self.schema)
        upsert_merge(self.spark, df_v1, ["id"], self.path, "storage")
        df_v2 = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)
        # Act
        upsert_merge(self.spark, df_v2, ["id"], self.path, "storage", has_delete=True)
        # Assert
        result = self.spark.read.format("delta").load(self.path)
        self.assertEqual(result.count(), 1)
        self.assertIsNone(result.filter("id = '2'").first())


class TestOverwrite(PySparkTestCase):
    """Testes para overwrite — substituição completa da tabela."""

    def setUp(self):
        self.path = self.unique_storage_path()
        schema = _schema_basico()
        self.spark.createDataFrame([("1", "Alice"), ("2", "Bob")], schema=schema).write.format("delta").mode(
            "overwrite"
        ).save(self.path)

    def test_overwrite_substitui_todos_os_dados(self):
        # Arrange
        schema = _schema_basico()
        df_novo = self.spark.createDataFrame([("3", "Charlie")], schema=schema)
        # Act
        overwrite(self.spark, df_novo, self.path, "storage")
        # Assert
        result = self.spark.read.format("delta").load(self.path)
        self.assertEqual(result.count(), 1)
        self.assertEqual(result.first()["nome"], "Charlie")
        self.assertIsNone(result.filter("id = '1'").first())


class TestOverwritePartition(PySparkTestCase):
    """Testes para overwrite_partition — substitui apenas a partição especificada."""

    def setUp(self):
        self.path = self.unique_storage_path()
        self.schema = StructType(
            [
                StructField("id", StringType()),
                StructField("nome", StringType()),
                StructField("year_month", StringType()),
            ]
        )
        self.spark.createDataFrame(
            [("1", "Alice", "2024-01"), ("2", "Bob", "2024-02")], schema=self.schema
        ).write.partitionBy("year_month").format("delta").mode("overwrite").save(self.path)

    def test_particao_alvo_e_substituida(self):
        # Arrange
        df_novo = self.spark.createDataFrame([("3", "Charlie", "2024-01")], schema=self.schema)
        # Act
        overwrite_partition(self.spark, df_novo, self.path, "year_month = '2024-01'", "storage")
        # Assert
        result = self.spark.read.format("delta").load(self.path).filter("year_month = '2024-01'")
        self.assertEqual(result.count(), 1)
        self.assertEqual(result.first()["nome"], "Charlie")

    def test_outras_particoes_nao_sao_afetadas(self):
        # Arrange
        df_novo = self.spark.createDataFrame([("3", "Charlie", "2024-01")], schema=self.schema)
        # Act
        overwrite_partition(self.spark, df_novo, self.path, "year_month = '2024-01'", "storage")
        # Assert
        result = self.spark.read.format("delta").load(self.path).filter("year_month = '2024-02'")
        self.assertEqual(result.count(), 1)
        self.assertEqual(result.first()["nome"], "Bob")

    def test_merge_cond_com_or_levanta_value_error(self):
        # Arrange — um 'OR' pode alargar o replaceWhere e apagar partições não-intencionais
        df_novo = self.spark.createDataFrame([("3", "Charlie", "2024-01")], schema=self.schema)

        # Act / Assert
        with self.assertRaises(ValueError):
            overwrite_partition(
                self.spark, df_novo, self.path, "year_month = '2024-01' OR year_month = '2024-02'", "storage"
            )

    def test_merge_cond_sem_coluna_de_particao_levanta_value_error(self):
        # Arrange — sem a coluna de partição (year_month) o replaceWhere apagaria tudo
        df_novo = self.spark.createDataFrame([("3", "Charlie", "2024-01")], schema=self.schema)

        # Act / Assert
        with self.assertRaises(ValueError):
            overwrite_partition(self.spark, df_novo, self.path, "id = '999'", "storage")


if __name__ == "__main__":
    unittest.main()
