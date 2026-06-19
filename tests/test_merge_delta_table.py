from datetime import datetime

from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType
from pyspark.testing.utils import assertDataFrameEqual

from gandalf import SCDColumns, SCDConfig
from gandalf.exceptions import DuplicatedDataFrameError, InvalidColumnsError
from gandalf.merge.merge_delta_table import merge_delta_table
from gandalf.scd.strategies import SCDValidationError
from tests.resources.utils import PySparkTestCase


def _schema_basico():
    return StructType([StructField("id", StringType()), StructField("nome", StringType())])


def _schema_scd_vazio(spark, table, sk_name=None):
    """Cria tabela Delta gerenciada vazia com schema SCD2 compatível com _schema_basico().

    Passe sk_name quando o teste usa sk_type — o Delta rejeita writes com colunas
    extras se o schema do target não as contiver (schema mismatch no overwrite inicial).
    """
    fields = [
        StructField("id", StringType()),
        StructField("nome", StringType()),
        StructField("scd_checksum", StringType()),
    ]
    if sk_name:
        fields.append(StructField(sk_name, StringType()))
    fields += [
        StructField("scd_is_current", IntegerType()),
        StructField("scd_start_dt", TimestampType()),
        StructField("scd_end_dt", TimestampType()),
    ]
    spark.createDataFrame([], StructType(fields)).write.format("delta").mode("overwrite").saveAsTable(table)


def _schema_upsert_vazio(spark, table, sk_name=None):
    """Cria tabela Delta gerenciada vazia para testes de upsert/overwrite."""
    fields = [StructField("id", StringType()), StructField("nome", StringType())]
    if sk_name:
        fields.append(StructField(sk_name, StringType()))
    spark.createDataFrame([], StructType(fields)).write.format("delta").mode("overwrite").saveAsTable(table)


class TestMergeDeltaTableValidacaoDeParametros(PySparkTestCase):
    """Valida que merge_delta_table rejeita parâmetros inválidos antes de qualquer operação Delta."""

    def _df(self):
        return self.spark.createDataFrame([("1", "Alice")], schema=_schema_basico())

    def test_merge_type_invalido_levanta_value_error(self):
        # Arrange / Act / Assert
        with self.assertRaises(ValueError) as ctx:
            merge_delta_table(self.spark, self._df(), "qualquer_path", ["id"], merge_type="invalido", path_type="table")
        self.assertIn("merge_type", str(ctx.exception))

    def test_sk_type_invalido_levanta_value_error(self):
        # Arrange / Act / Assert
        with self.assertRaises(ValueError) as ctx:
            merge_delta_table(
                self.spark,
                self._df(),
                "qualquer_path",
                ["id"],
                sk_type="invalido",
                sk_name="sk_x",
                path_type="table",
            )
        self.assertIn("sk_type", str(ctx.exception))

    def test_path_type_invalido_levanta_value_error(self):
        # Arrange / Act / Assert
        with self.assertRaises(ValueError) as ctx:
            merge_delta_table(self.spark, self._df(), "qualquer_path", ["id"], path_type="invalido")
        self.assertIn("path_type", str(ctx.exception))

    def test_sk_name_sem_sk_type_levanta_value_error(self):
        # Arrange / Act / Assert
        with self.assertRaises(ValueError):
            merge_delta_table(self.spark, self._df(), "qualquer_path", ["id"], sk_name="sk_x", path_type="table")

    def test_sk_type_sem_sk_name_levanta_value_error(self):
        # Arrange / Act / Assert
        with self.assertRaises(ValueError):
            merge_delta_table(self.spark, self._df(), "qualquer_path", ["id"], sk_type="dim", path_type="table")

    def test_delta_col_filter_com_scd_levanta_value_error(self):
        # Arrange / Act / Assert
        with self.assertRaises(ValueError):
            merge_delta_table(
                self.spark,
                self._df(),
                "qualquer_path",
                ["id"],
                merge_type="scd",
                delta_col_filter="updated_at",
                path_type="table",
            )

    def test_ignore_update_cols_com_scd_levanta_value_error(self):
        # Arrange / Act / Assert
        with self.assertRaises(ValueError):
            merge_delta_table(
                self.spark,
                self._df(),
                "qualquer_path",
                ["id"],
                merge_type="scd",
                ignore_update_cols=["nome"],
                path_type="table",
            )

    def test_ignore_update_cols_com_scd_no_delete_levanta_value_error(self):
        # Arrange / Act / Assert
        with self.assertRaises(ValueError):
            merge_delta_table(
                self.spark,
                self._df(),
                "qualquer_path",
                ["id"],
                merge_type="scd_no_delete",
                ignore_update_cols=["nome"],
                path_type="table",
            )


class TestMergeDeltaTableScd(PySparkTestCase):
    """Integração do pipeline SCD2 via merge_delta_table."""

    def setUp(self):
        self.table = self.unique_table()
        _schema_scd_vazio(self.spark, self.table)
        self.schema = _schema_basico()

    def test_scd_carga_inicial(self):
        # Arrange
        df = self.spark.createDataFrame([("1", "Alice"), ("2", "Bob")], schema=self.schema)

        # Act
        merge_delta_table(self.spark, df, self.table, ["id"], merge_type="scd", path_type="table")

        # Assert
        result = self.spark.read.table(self.table).filter("scd_is_current = 1").select("id", "nome")
        expected = self.spark.createDataFrame([("1", "Alice"), ("2", "Bob")], schema=self.schema)
        assertDataFrameEqual(result, expected)

    def test_scd_linha_alterada_cria_nova_versao(self):
        # Arrange
        df_v1 = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)
        merge_delta_table(self.spark, df_v1, self.table, ["id"], merge_type="scd", path_type="table")

        # Act
        df_v2 = self.spark.createDataFrame([("1", "Alice Atualizada")], schema=self.schema)
        merge_delta_table(self.spark, df_v2, self.table, ["id"], merge_type="scd", path_type="table")

        # Assert
        result = self.spark.read.table(self.table)
        self.assertEqual(result.filter("id = '1'").count(), 2)

        current = result.filter("id = '1' AND scd_is_current = 1").select("id", "nome")
        expected = self.spark.createDataFrame([("1", "Alice Atualizada")], schema=self.schema)
        assertDataFrameEqual(current, expected)

    def test_scd_no_delete_nao_marca_ausentes(self):
        # Arrange
        df_v1 = self.spark.createDataFrame([("1", "Alice"), ("2", "Bob")], schema=self.schema)
        merge_delta_table(self.spark, df_v1, self.table, ["id"], merge_type="scd_no_delete", path_type="table")

        # Act
        df_v2 = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)
        merge_delta_table(self.spark, df_v2, self.table, ["id"], merge_type="scd_no_delete", path_type="table")

        # Assert
        result_bob = self.spark.read.table(self.table).filter("id = '2' AND scd_is_current = 1").select("id", "nome")
        expected = self.spark.createDataFrame([("2", "Bob")], schema=self.schema)
        assertDataFrameEqual(result_bob, expected)

    def test_scd_rerun_identico_e_idempotente(self):
        # Arrange
        df = self.spark.createDataFrame([("1", "Alice"), ("2", "Bob")], schema=self.schema)
        merge_delta_table(self.spark, df, self.table, ["id"], merge_type="scd", path_type="table")

        # Act — rerun idêntico não deve criar novas versões
        merge_delta_table(self.spark, df, self.table, ["id"], merge_type="scd", path_type="table")

        # Assert
        result = self.spark.read.table(self.table)
        self.assertEqual(result.count(), 2)
        self.assertEqual(result.filter("scd_is_current = 1").count(), 2)

    def test_checksum_ignore_list_nao_cria_nova_versao_para_metadados(self):
        # Arrange
        schema = StructType(
            [
                StructField("id", StringType()),
                StructField("nome", StringType()),
                StructField("created_at", StringType()),
            ]
        )
        table = self.unique_table()
        scd_schema = StructType(
            [
                StructField("id", StringType()),
                StructField("nome", StringType()),
                StructField("created_at", StringType()),
                StructField("scd_checksum", StringType()),
                StructField("scd_is_current", IntegerType()),
                StructField("scd_start_dt", TimestampType()),
                StructField("scd_end_dt", TimestampType()),
            ]
        )
        self.spark.createDataFrame([], scd_schema).write.format("delta").mode("overwrite").saveAsTable(table)
        df_v1 = self.spark.createDataFrame([("1", "Alice", "2024-01-01")], schema=schema)
        merge_delta_table(
            self.spark,
            df_v1,
            table,
            ["id"],
            merge_type="scd",
            checksum_ignore_list=["created_at"],
            path_type="table",
        )

        # Act
        df_v2 = self.spark.createDataFrame([("1", "Alice", "2024-12-31")], schema=schema)
        merge_delta_table(
            self.spark,
            df_v2,
            table,
            ["id"],
            merge_type="scd",
            checksum_ignore_list=["created_at"],
            path_type="table",
        )

        # Assert
        result = self.spark.read.table(table)
        self.assertEqual(result.count(), 1)

        current = result.select("id", "nome")
        expected = self.spark.createDataFrame(
            [("1", "Alice")],
            schema=StructType([StructField("id", StringType()), StructField("nome", StringType())]),
        )
        assertDataFrameEqual(current, expected)


class TestMergeDeltaTableUpsert(PySparkTestCase):
    """Integração do pipeline upsert via merge_delta_table."""

    def setUp(self):
        self.table = self.unique_table()
        _schema_upsert_vazio(self.spark, self.table)
        self.schema = _schema_basico()

    def test_upsert_insere_e_atualiza(self):
        # Arrange
        df_v1 = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)
        merge_delta_table(self.spark, df_v1, self.table, ["id"], merge_type="upsert", path_type="table")

        # Act
        df_v2 = self.spark.createDataFrame([("1", "Alice Atualizada"), ("2", "Bob")], schema=self.schema)
        merge_delta_table(self.spark, df_v2, self.table, ["id"], merge_type="upsert", path_type="table")

        # Assert
        result = self.spark.read.table(self.table).select("id", "nome")
        expected = self.spark.createDataFrame([("1", "Alice Atualizada"), ("2", "Bob")], schema=self.schema)
        assertDataFrameEqual(result, expected)

    def test_duplicatas_no_source_levantam_erro(self):
        # Arrange — o pré-check de duplicatas no source (step 3) roda sempre
        df_com_dupes = self.spark.createDataFrame([("1", "Alice"), ("1", "Alice Dup")], schema=self.schema)

        # Act / Assert
        with self.assertRaises(DuplicatedDataFrameError):
            merge_delta_table(self.spark, df_com_dupes, self.table, ["id"], merge_type="upsert", path_type="table")

    def test_upsert_ignore_update_cols_preserva_valor_original(self):
        # Arrange
        schema = StructType(
            [
                StructField("id", StringType()),
                StructField("nome", StringType()),
                StructField("created_at", StringType()),
            ]
        )
        table = self.unique_table()
        self.spark.createDataFrame([], schema).write.format("delta").mode("overwrite").saveAsTable(table)
        df_v1 = self.spark.createDataFrame([("1", "Alice", "2024-01-01")], schema=schema)
        merge_delta_table(self.spark, df_v1, table, ["id"], merge_type="upsert", path_type="table")

        # Act
        df_v2 = self.spark.createDataFrame([("1", "Alice Atualizada", "2024-12-31")], schema=schema)
        merge_delta_table(
            self.spark,
            df_v2,
            table,
            ["id"],
            merge_type="upsert",
            ignore_update_cols=["created_at"],
            path_type="table",
        )

        # Assert
        row = self.spark.read.table(table).filter("id = '1'").first()
        self.assertEqual(row["nome"], "Alice Atualizada")
        self.assertEqual(row["created_at"], "2024-01-01")


class TestMergeDeltaTableSk(PySparkTestCase):
    """Testes de geração de surrogate keys via merge_delta_table."""

    def setUp(self):
        self.schema = _schema_basico()

    def _table_scd(self):
        table = self.unique_table()
        _schema_scd_vazio(self.spark, table, sk_name="sk_entidade")
        return table

    def _table_upsert(self):
        table = self.unique_table()
        _schema_upsert_vazio(self.spark, table, sk_name="sk_fact")
        return table

    def test_dim_sk_gerada_com_64_hex(self):
        # Arrange
        table = self._table_scd()
        df = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)

        # Act
        merge_delta_table(
            self.spark,
            df,
            table,
            ["id"],
            merge_type="scd",
            sk_type="dim",
            sk_name="sk_entidade",
            path_type="table",
        )

        # Assert
        sk = self.spark.read.table(table).filter("scd_is_current = 1").first()["sk_entidade"]
        self.assertIsNotNone(sk)
        self.assertEqual(len(sk), 64)
        self.assertRegex(sk, r"^[0-9a-f]{64}$")

    def test_fact_sk_e_deterministica(self):
        # Arrange
        table = self._table_upsert()
        df = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)

        # Act
        merge_delta_table(
            self.spark,
            df,
            table,
            ["id"],
            merge_type="upsert",
            sk_type="fact",
            sk_name="sk_fact",
            path_type="table",
        )
        sk_primeira = self.spark.read.table(table).filter("id = '1'").first()["sk_fact"]

        # segunda chamada — SK deve ser idêntica (determinística)
        merge_delta_table(
            self.spark,
            df,
            table,
            ["id"],
            merge_type="upsert",
            sk_type="fact",
            sk_name="sk_fact",
            path_type="table",
        )
        sk_segunda = self.spark.read.table(table).filter("id = '1'").first()["sk_fact"]

        # Assert
        self.assertEqual(sk_primeira, sk_segunda)

    def test_sk_nao_dispara_nova_versao_scd(self):
        """SK dim muda a cada execução (inclui timestamp) — não deve afetar o check_sum de negócio."""
        # Arrange
        table = self._table_scd()
        df = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)

        # Act
        merge_delta_table(
            self.spark,
            df,
            table,
            ["id"],
            merge_type="scd",
            sk_type="dim",
            sk_name="sk_entidade",
            path_type="table",
        )
        merge_delta_table(
            self.spark,
            df,
            table,
            ["id"],
            merge_type="scd",
            sk_type="dim",
            sk_name="sk_entidade",
            path_type="table",
        )

        # Assert
        result = self.spark.read.table(table).filter("id = '1'")
        self.assertEqual(result.count(), 1)


class TestMergeDeltaTableDeltaColFilter(PySparkTestCase):
    """Testa o filtro incremental via delta_col_filter (step 2 do pipeline)."""

    def setUp(self):
        self.table = self.unique_table()
        self.schema = StructType(
            [
                StructField("id", StringType()),
                StructField("nome", StringType()),
                StructField("updated_at", StringType()),
            ]
        )
        self.spark.createDataFrame([], self.schema).write.format("delta").mode("overwrite").saveAsTable(self.table)

    def _schema_simples(self):
        return StructType([StructField("id", StringType()), StructField("nome", StringType())])

    def test_delta_col_filter_processa_apenas_linhas_novas(self):
        # Arrange
        df_v1 = self.spark.createDataFrame([("1", "Alice", "2024-01-01")], schema=self.schema)
        merge_delta_table(self.spark, df_v1, self.table, ["id"], merge_type="upsert", path_type="table")

        # Act — linha com data igual à do target é filtrada; linha nova passa
        df_v2 = self.spark.createDataFrame(
            [("1", "Alice Ignorada", "2024-01-01"), ("2", "Bob", "2024-06-01")],
            schema=self.schema,
        )
        merge_delta_table(
            self.spark,
            df_v2,
            self.table,
            ["id"],
            merge_type="upsert",
            delta_col_filter="updated_at",
            path_type="table",
        )

        # Assert
        result = self.spark.read.table(self.table).select("id", "nome")
        expected = self.spark.createDataFrame([("1", "Alice"), ("2", "Bob")], schema=self._schema_simples())
        assertDataFrameEqual(result, expected)

    def test_delta_col_filter_com_target_vazio_processa_todas_as_linhas(self):
        # Arrange
        df = self.spark.createDataFrame(
            [("1", "Alice", "2024-01-01"), ("2", "Bob", "2024-06-01")],
            schema=self.schema,
        )

        # Act
        merge_delta_table(
            self.spark,
            df,
            self.table,
            ["id"],
            merge_type="upsert",
            delta_col_filter="updated_at",
            path_type="table",
        )

        # Assert
        result = self.spark.read.table(self.table).select("id", "nome")
        expected = self.spark.createDataFrame([("1", "Alice"), ("2", "Bob")], schema=self._schema_simples())
        assertDataFrameEqual(result, expected)


class TestMergeDeltaTableMissingMergeTypes(PySparkTestCase):
    """Cobre os merge types não testados nas demais classes."""

    def setUp(self):
        self.table = self.unique_table()
        self.schema = _schema_basico()
        _schema_upsert_vazio(self.spark, self.table)

    def test_overwrite_substitui_todos_os_dados(self):
        # Arrange
        df_v1 = self.spark.createDataFrame([("1", "Alice"), ("2", "Bob")], schema=self.schema)
        merge_delta_table(self.spark, df_v1, self.table, ["id"], merge_type="overwrite", path_type="table")

        # Act
        df_v2 = self.spark.createDataFrame([("3", "Carol")], schema=self.schema)
        merge_delta_table(self.spark, df_v2, self.table, ["id"], merge_type="overwrite", path_type="table")

        # Assert
        result = self.spark.read.table(self.table).select("id", "nome")
        expected = self.spark.createDataFrame([("3", "Carol")], schema=self.schema)
        assertDataFrameEqual(result, expected)

    def test_upsert_delete_remove_linha_ausente_do_source(self):
        # Arrange
        df_v1 = self.spark.createDataFrame([("1", "Alice"), ("2", "Bob")], schema=self.schema)
        merge_delta_table(self.spark, df_v1, self.table, ["id"], merge_type="upsert", path_type="table")

        # Act — Bob ausente do source — deve ser removido
        df_v2 = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)
        merge_delta_table(self.spark, df_v2, self.table, ["id"], merge_type="upsert-delete", path_type="table")

        # Assert
        result = self.spark.read.table(self.table).select("id", "nome")
        expected = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)
        assertDataFrameEqual(result, expected)

    def test_overwrite_partition_substitui_apenas_particao_alvo(self):
        # Arrange
        schema = StructType(
            [
                StructField("id", StringType()),
                StructField("nome", StringType()),
                StructField("year_month", StringType()),
            ]
        )
        table = self.unique_table()
        self.spark.createDataFrame(
            [("1", "Alice", "2024-01"), ("2", "Bob", "2024-02")],
            schema=schema,
        ).write.format("delta").mode("overwrite").partitionBy("year_month").saveAsTable(table)

        # Act
        df_source = self.spark.createDataFrame([("3", "Carol", "2024-01")], schema=schema)
        merge_delta_table(
            self.spark,
            df_source,
            table,
            ["id"],
            merge_type="overwrite-partition",
            merge_cond="year_month = '2024-01'",
            path_type="table",
        )

        # Assert
        result = self.spark.read.table(table).select("id", "nome", "year_month")
        expected = self.spark.createDataFrame(
            [("3", "Carol", "2024-01"), ("2", "Bob", "2024-02")],
            schema=schema,
        )
        assertDataFrameEqual(result, expected)


class TestMergeDeltaTableScdColumns(PySparkTestCase):
    """Custom SCD control-column names must be honored end-to-end (not just stored)."""

    def setUp(self):
        self.schema = _schema_basico()
        # Fully custom names — none match gandalf's scd_ defaults — to prove the names
        # are threaded through scd2_batch and the post-merge validators, not hardcoded.
        self.columns = SCDColumns(
            start_dt="dt_inicio",
            end_dt="dt_fim",
            is_current="situacao_atual",
            checksum="hash_negocio",
        )
        self.table = self.unique_table()
        fields = [
            StructField("id", StringType()),
            StructField("nome", StringType()),
            StructField("hash_negocio", StringType()),
            StructField("situacao_atual", IntegerType()),
            StructField("dt_inicio", TimestampType()),
            StructField("dt_fim", TimestampType()),
        ]
        self.spark.createDataFrame([], StructType(fields)).write.format("delta").mode("overwrite").saveAsTable(
            self.table
        )

    def test_scd_com_nomes_de_coluna_customizados(self):
        # Arrange
        df_v1 = self.spark.createDataFrame([("1", "Alice"), ("2", "Bob")], schema=self.schema)
        merge_delta_table(self.spark, df_v1, self.table, ["id"], merge_type="scd", scd_columns=self.columns)

        # Act — altera uma linha; deve criar nova versão usando os nomes customizados
        df_v2 = self.spark.createDataFrame([("1", "Alice Atualizada"), ("2", "Bob")], schema=self.schema)
        merge_delta_table(self.spark, df_v2, self.table, ["id"], merge_type="scd", scd_columns=self.columns)

        # Assert
        result = self.spark.read.table(self.table)
        self.assertEqual(result.filter("id = '1'").count(), 2)
        current = result.filter("id = '1' AND situacao_atual = 1").select("id", "nome")
        expected = self.spark.createDataFrame([("1", "Alice Atualizada")], schema=self.schema)
        assertDataFrameEqual(current, expected)

    def test_target_sem_colunas_de_controle_levanta_invalid_columns(self):
        # Arrange — target NÃO-vazio sem as colunas de controle SCD
        table = self.unique_table()
        self.spark.createDataFrame([("1", "Alice")], schema=self.schema).write.format("delta").mode(
            "overwrite"
        ).saveAsTable(table)
        df = self.spark.createDataFrame([("2", "Bob")], schema=self.schema)

        # Act / Assert
        with self.assertRaises(InvalidColumnsError) as ctx:
            merge_delta_table(self.spark, df, table, ["id"], merge_type="scd")
        self.assertIn("scd_is_current", str(ctx.exception))

    def test_scd_columns_e_scd_config_juntos_levanta_value_error(self):
        # Arrange
        df = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)

        # Act / Assert — a checagem ocorre antes de qualquer operação Delta
        with self.assertRaises(ValueError) as ctx:
            merge_delta_table(
                self.spark,
                df,
                self.table,
                ["id"],
                merge_type="scd",
                scd_columns=SCDColumns(),
                scd_config=SCDConfig(scd_type="scd2", ids=["id"]),
            )
        self.assertIn("not both", str(ctx.exception))


def _schema_scd6_target():
    return StructType(
        [
            StructField("id", StringType()),
            StructField("nome", StringType()),
            StructField("prev_nome", StringType()),
            StructField("scd_checksum", StringType()),
            StructField("scd_is_current", IntegerType()),
            StructField("scd_start_dt", TimestampType()),
            StructField("scd_end_dt", TimestampType()),
        ]
    )


class TestMergeDeltaTableScd6(PySparkTestCase):
    """SCD6 via scd_config — Type 2 versioning + previous-value column."""

    def setUp(self):
        self.schema = _schema_basico()
        self.config = SCDConfig(scd_type="scd6", ids=["id"], previous_columns={"nome": "prev_nome"})
        self.table = self.unique_table()
        self.spark.createDataFrame([], _schema_scd6_target()).write.format("delta").mode("overwrite").saveAsTable(
            self.table
        )

    def test_scd6_versiona_e_preenche_coluna_anterior(self):
        # Arrange
        df_v1 = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)
        merge_delta_table(self.spark, df_v1, self.table, ["id"], scd_config=self.config, path_type="table")

        # Act
        df_v2 = self.spark.createDataFrame([("1", "Alice Atualizada")], schema=self.schema)
        merge_delta_table(self.spark, df_v2, self.table, ["id"], scd_config=self.config, path_type="table")

        # Assert
        result = self.spark.read.table(self.table)
        self.assertEqual(result.filter("id = '1'").count(), 2)
        current = result.filter("id = '1' AND scd_is_current = 1").first()
        self.assertEqual(current["nome"], "Alice Atualizada")
        self.assertEqual(current["prev_nome"], "Alice")

    def test_scd6_target_com_current_duplicado_levanta_duplicated_error(self):
        # Arrange — target corrompido: duas linhas current para o mesmo id faria o left join
        # do scd6 fanar em chaves duplicadas; o guard de dedup deve falhar cedo e claro.
        table = self.unique_table()
        rows = [
            ("1", "Alice", None, "h1", 1, datetime(2024, 1, 1), datetime(2025, 1, 1)),
            ("1", "Alicia", None, "h2", 1, datetime(2024, 1, 1), datetime(2025, 1, 1)),
        ]
        self.spark.createDataFrame(rows, schema=_schema_scd6_target()).write.format("delta").mode(
            "overwrite"
        ).saveAsTable(table)
        df = self.spark.createDataFrame([("1", "Bob")], schema=self.schema)

        # Act / Assert
        with self.assertRaises(DuplicatedDataFrameError):
            merge_delta_table(self.spark, df, table, ["id"], scd_config=self.config, path_type="table")


class TestMergeDeltaTableScd0(PySparkTestCase):
    """SCD0 via scd_config — insert new keys only; protected columns must not change."""

    def setUp(self):
        self.schema = _schema_basico()
        self.config = SCDConfig(scd_type="scd0", ids=["id"], protected_columns=["nome"])
        self.table = self.unique_table()
        _schema_upsert_vazio(self.spark, self.table)

    def test_scd0_insere_novas_chaves_e_ignora_existentes_inalteradas(self):
        # Arrange
        df_v1 = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)
        merge_delta_table(self.spark, df_v1, self.table, ["id"], scd_config=self.config, path_type="table")

        # Act — id=1 inalterada (protegida OK), id=2 nova
        df_v2 = self.spark.createDataFrame([("1", "Alice"), ("2", "Bob")], schema=self.schema)
        merge_delta_table(self.spark, df_v2, self.table, ["id"], scd_config=self.config, path_type="table")

        # Assert
        result = self.spark.read.table(self.table).select("id", "nome")
        expected = self.spark.createDataFrame([("1", "Alice"), ("2", "Bob")], schema=self.schema)
        assertDataFrameEqual(result, expected)

    def test_scd0_coluna_protegida_alterada_levanta_erro(self):
        # Arrange
        df_v1 = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)
        merge_delta_table(self.spark, df_v1, self.table, ["id"], scd_config=self.config, path_type="table")

        # Act / Assert — alterar a coluna protegida de uma chave existente é rejeitado
        df_v2 = self.spark.createDataFrame([("1", "Alterada")], schema=self.schema)
        with self.assertRaises(SCDValidationError):
            merge_delta_table(self.spark, df_v2, self.table, ["id"], scd_config=self.config, path_type="table")


def _schema_scd3_target():
    return StructType(
        [
            StructField("id", StringType()),
            StructField("nome", StringType()),
            StructField("prev_nome", StringType()),
        ]
    )


class TestMergeDeltaTableScd3(PySparkTestCase):
    """SCD3 via scd_config — update current value, shift the old value into a previous column."""

    def setUp(self):
        self.schema = _schema_basico()
        self.config = SCDConfig(scd_type="scd3", ids=["id"], previous_columns={"nome": "prev_nome"})
        self.table = self.unique_table()
        self.spark.createDataFrame([], _schema_scd3_target()).write.format("delta").mode("overwrite").saveAsTable(
            self.table
        )

    def test_scd3_carga_inicial_insere_com_anterior_nulo(self):
        # Arrange / Act
        df_v1 = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)
        merge_delta_table(self.spark, df_v1, self.table, ["id"], scd_config=self.config, path_type="table")

        # Assert
        row = self.spark.read.table(self.table).filter("id = '1'").first()
        self.assertEqual(row["nome"], "Alice")
        self.assertIsNone(row["prev_nome"])

    def test_scd3_atualiza_e_move_valor_para_coluna_anterior(self):
        # Arrange
        df_v1 = self.spark.createDataFrame([("1", "Alice")], schema=self.schema)
        merge_delta_table(self.spark, df_v1, self.table, ["id"], scd_config=self.config, path_type="table")

        # Act
        df_v2 = self.spark.createDataFrame([("1", "Alicia")], schema=self.schema)
        merge_delta_table(self.spark, df_v2, self.table, ["id"], scd_config=self.config, path_type="table")

        # Assert
        result = self.spark.read.table(self.table)
        self.assertEqual(result.filter("id = '1'").count(), 1)
        row = result.filter("id = '1'").first()
        self.assertEqual(row["nome"], "Alicia")
        self.assertEqual(row["prev_nome"], "Alice")
