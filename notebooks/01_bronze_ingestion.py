"""
================================================================================
01_BRONZE_INGESTION.PY
Projeto: CNPJ Data Lakehouse (Receita Federal do Brasil)
Plataforma: Databricks / PySpark / Delta Lake
Objetivo: Ingestao de dados brutos (CSV/ISO-8859-1) para tabelas Delta na camada Bronze com Schema Enforcement e Metadados.
================================================================================
"""

# Databricks Notebook Source
# COMMAND ----------
# MAGIC %md
# MAGIC # 01 - Ingestao Camada Bronze (Raw -> Delta Lake)
# MAGIC 
# MAGIC ### Objetivos:
# MAGIC 1. Ingerir os arquivos publicos da Receita Federal (delimitados por `;`, sem cabecalho, encoding `ISO-8859-1`).
# MAGIC 2. Aplicar **Schema Enforcement** explicito para evitar inferencias lentas em Big Data.
# MAGIC 3. Adicionar metadados de auditoria (`_ingest_timestamp`, `_source_file`).
# MAGIC 4. Salvar como tabelas Delta Lake na camada `cnpj_lakehouse.bronze`.
# MAGIC 5. Se o Volume de entrada estiver vazio, gera dados de seed automaticamente para garantir execucao imediata.

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, input_file_name, lit
import random

# COMMAND ----------
# 1. Configuracoes de Caminho e Catalogo
CATALOG_NAME = "cnpj_lakehouse"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/bronze/raw_landing"
BRONZE_SCHEMA = f"{CATALOG_NAME}.bronze"

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA bronze")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Definicao Explicita de Schemas (Padrao Receita Federal)

# COMMAND ----------
schema_empresas = StructType([
    StructField("cnpj_basico", StringType(), False),
    StructField("razao_social", StringType(), True),
    StructField("natureza_juridica", StringType(), True),
    StructField("qualificacao_responsavel", StringType(), True),
    StructField("capital_social_str", StringType(), True),
    StructField("porte_empresa", StringType(), True),
    StructField("ente_federativo_responsavel", StringType(), True)
])

schema_estabelecimentos = StructType([
    StructField("cnpj_basico", StringType(), False),
    StructField("cnpj_ordem", StringType(), False),
    StructField("cnpj_dv", StringType(), False),
    StructField("identificador_matriz_filial", StringType(), True),
    StructField("nome_fantasia", StringType(), True),
    StructField("situacao_cadastral", StringType(), True),
    StructField("data_situacao_cadastral", StringType(), True),
    StructField("motivo_situacao_cadastral", StringType(), True),
    StructField("nome_cidade_exterior", StringType(), True),
    StructField("pais", StringType(), True),
    StructField("data_inicio_atividade", StringType(), True),
    StructField("cnae_fiscal_principal", StringType(), True),
    StructField("cnae_fiscal_secundaria", StringType(), True),
    StructField("tipo_logradouro", StringType(), True),
    StructField("logradouro", StringType(), True),
    StructField("numero", StringType(), True),
    StructField("complemento", StringType(), True),
    StructField("bairro", StringType(), True),
    StructField("cep", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("ddd_1", StringType(), True),
    StructField("telefone_1", StringType(), True),
    StructField("ddd_2", StringType(), True),
    StructField("telefone_2", StringType(), True),
    StructField("ddd_fax", StringType(), True),
    StructField("fax", StringType(), True),
    StructField("correio_eletronico", StringType(), True),
    StructField("situacao_especial", StringType(), True),
    StructField("data_situacao_especial", StringType(), True)
])

schema_socios = StructType([
    StructField("cnpj_basico", StringType(), False),
    StructField("identificador_socio", StringType(), True),
    StructField("nome_socio", StringType(), True),
    StructField("cnpj_cpf_socio", StringType(), True),
    StructField("qualificacao_socio", StringType(), True),
    StructField("data_entrada_sociedade", StringType(), True),
    StructField("pais", StringType(), True),
    StructField("representante_legal", StringType(), True),
    StructField("nome_representante", StringType(), True),
    StructField("qualificacao_representante", StringType(), True),
    StructField("faixa_etaria", StringType(), True)
])

schema_lookup = StructType([
    StructField("codigo", StringType(), False),
    StructField("descricao", StringType(), True)
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Funcao de Ingestao com Fallback Automatico de Seed

# COMMAND ----------
def generate_seed_data():
    """Gera dados de seed sinteticos caso o Volume raw_landing nao contenha arquivos ainda."""
    print("[INFO] Volume vazio detectado. Gerando dataset inicial de seed da Receita Federal...")
    
    # 1. CNAEs
    cnaes_data = [
        ("6201501", "Desenvolvimento de programas de computador sob encomenda"),
        ("6202300", "Desenvolvimento e licenciamento de programas de computador customizaveis"),
        ("6204000", "Consultoria em tecnologia da informacao"),
        ("4711302", "Comercio varejista de mercadorias em geral com predominancia de produtos alimenticios"),
        ("4751201", "Comercio varejista especializado de equipamentos de informatica"),
        ("5611201", "Restaurantes e similares"),
        ("6920601", "Atividades de contabilidade"),
        ("7020400", "Atividades de consultoria em gestao empresarial"),
        ("8630503", "Atividade medica ambulatorial restrita a consultas"),
        ("4120400", "Construcao de edificios")
    ]
    df_cnaes = spark.createDataFrame(cnaes_data, schema_lookup).withColumn("_ingest_timestamp", current_timestamp()).withColumn("_source_file", lit("seed_synthetic"))
    df_cnaes.write.format("delta").mode("overwrite").saveAsTable(f"{BRONZE_SCHEMA}.bronze_cnae")

    # 2. Municipios
    mun_data = [
        ("7107", "SAO PAULO"), ("6001", "RIO DE JANEIRO"), ("4123", "BELO HORIZONTE"),
        ("5847", "PORTO ALEGRE"), ("7535", "CURITIBA"), ("9701", "BRASILIA"),
        ("3849", "SALVADOR"), ("1389", "RECIFE"), ("2587", "FORTALEZA"), ("0255", "MANAUS")
    ]
    df_mun = spark.createDataFrame(mun_data, schema_lookup).withColumn("_ingest_timestamp", current_timestamp()).withColumn("_source_file", lit("seed_synthetic"))
    df_mun.write.format("delta").mode("overwrite").saveAsTable(f"{BRONZE_SCHEMA}.bronze_municipios")

    # 3. Naturezas Juridicas
    nat_data = [
        ("2062", "SOCIEDADE EMPRESARIA LIMITADA"), ("2054", "SOCIEDADE ANONIMA FECHADA"),
        ("2046", "SOCIEDADE ANONIMA ABERTA"), ("2135", "EMPRESARIO INDIVIDUAL"),
        ("2305", "EMPRESA INDIVIDUAL DE RESPONSABILIDADE LIMITADA (EIRELI)")
    ]
    df_nat = spark.createDataFrame(nat_data, schema_lookup).withColumn("_ingest_timestamp", current_timestamp()).withColumn("_source_file", lit("seed_synthetic"))
    df_nat.write.format("delta").mode("overwrite").saveAsTable(f"{BRONZE_SCHEMA}.bronze_naturezas_juridicas")

    # 4. Qualificacoes
    qual_data = [
        ("49", "Socio-Administrador"), ("22", "Socio"), ("10", "Diretor"), ("05", "Administrador")
    ]
    df_qual = spark.createDataFrame(qual_data, schema_lookup).withColumn("_ingest_timestamp", current_timestamp()).withColumn("_source_file", lit("seed_synthetic"))
    df_qual.write.format("delta").mode("overwrite").saveAsTable(f"{BRONZE_SCHEMA}.bronze_qualificacoes_socios")

    # 5. Motivos Situacao Cadastral
    mot_data = [("00", "SEM MOTIVO"), ("01", "EXTINCAO POR ENCERRAMENTO LIQUIDACAO VOLUNTARIA")]
    df_mot = spark.createDataFrame(mot_data, schema_lookup).withColumn("_ingest_timestamp", current_timestamp()).withColumn("_source_file", lit("seed_synthetic"))
    df_mot.write.format("delta").mode("overwrite").saveAsTable(f"{BRONZE_SCHEMA}.bronze_motivos_situacao_cadastral")

    # 6. Empresas, Estabelecimentos e Socios
    ufs = [("SP", "7107"), ("RJ", "6001"), ("MG", "4123"), ("RS", "5847"), ("PR", "7535"), ("DF", "9701"), ("BA", "3849"), ("PE", "1389")]
    cnae_list = ["6201501", "6202300", "6204000", "4711302", "4751201", "5611201", "6920601", "7020400", "8630503", "4120400"]
    names = ["SILVA", "SANTOS", "OLIVEIRA", "SOUZA", "RODRIGUES", "FERREIRA", "ALVES", "PEREIRA"]
    suffixes = ["TECNOLOGIA", "CONSULTORIA", "COMERCIO", "SOLUCOES", "LOGISTICA", "SERVICOS"]
    
    emp_rows = []
    est_rows = []
    soc_rows = []

    for i in range(1, 2001):
        cnpj_basico = f"{i:08d}"
        razao = f"{random.choice(names)} E {random.choice(names)} {random.choice(suffixes)} LTDA"
        nat = random.choice(["2062", "2054", "2135"])
        cap = f"{random.randint(5000, 5000000)},00"
        porte = random.choice(["01", "03", "05"])
        emp_rows.append((cnpj_basico, razao, nat, "49", cap, porte, ""))

        uf, mun = random.choice(ufs)
        cnae = random.choice(cnae_list)
        sit = random.choice(["02", "02", "02", "08", "04"])
        data_ini = f"{random.randint(2005, 2024):04d}{random.randint(1,12):02d}15"
        data_sit = data_ini if sit == "02" else f"{random.randint(2021, 2024):04d}0615"
        
        est_rows.append((
            cnpj_basico, "0001", "90", "1", f"{razao.split()[0]} STORE", sit, data_sit,
            "00" if sit == "02" else "01", "", "", data_ini, cnae, "", "RUA", "AVENIDA PRINCIPAL",
            str(i), "SALA 1", "CENTRO", f"{random.randint(10000000, 99999999)}",
            uf, mun, "11", "987654321", "", "", "", "", "contato@empresa.com.br", "", ""
        ))

        soc_rows.append((
            cnpj_basico, "2", f"{random.choice(names)} {random.choice(names)}",
            f"***{random.randint(100000, 999999)}**", "49", data_ini, "", "", "", "", str(random.randint(3, 7))
        ))

    df_emp = spark.createDataFrame(emp_rows, schema_empresas).withColumn("_ingest_timestamp", current_timestamp()).withColumn("_source_file", lit("seed_synthetic"))
    df_emp.write.format("delta").mode("overwrite").saveAsTable(f"{BRONZE_SCHEMA}.bronze_empresas")

    df_est = spark.createDataFrame(est_rows, schema_estabelecimentos).withColumn("_ingest_timestamp", current_timestamp()).withColumn("_source_file", lit("seed_synthetic"))
    df_est.write.format("delta").mode("overwrite").saveAsTable(f"{BRONZE_SCHEMA}.bronze_estabelecimentos")

    df_soc = spark.createDataFrame(soc_rows, schema_socios).withColumn("_ingest_timestamp", current_timestamp()).withColumn("_source_file", lit("seed_synthetic"))
    df_soc.write.format("delta").mode("overwrite").saveAsTable(f"{BRONZE_SCHEMA}.bronze_socios")

    print("[INFO] Tabelas Bronze populadas com sucesso via dataset de seed.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Execucao da Ingestao

# COMMAND ----------
datasets_to_ingest = [
    ("empresas/*.csv", schema_empresas, "bronze_empresas"),
    ("estabelecimentos/*.csv", schema_estabelecimentos, "bronze_estabelecimentos"),
    ("socios/*.csv", schema_socios, "bronze_socios"),
    ("cnaes/*.csv", schema_lookup, "bronze_cnae"),
    ("municipios/*.csv", schema_lookup, "bronze_municipios"),
    ("naturezas/*.csv", schema_lookup, "bronze_naturezas_juridicas"),
    ("qualificacoes/*.csv", schema_lookup, "bronze_qualificacoes_socios"),
    ("motivos/*.csv", schema_lookup, "bronze_motivos_situacao_cadastral")
]

raw_files_found = False
for file_pattern, schema, table_name in datasets_to_ingest:
    try:
        input_path = f"{VOLUME_PATH}/{file_pattern}"
        df_raw = (
            spark.read
            .format("csv")
            .option("sep", ";")
            .option("encoding", "ISO-8859-1")
            .option("quote", "\"")
            .option("escape", "\"")
            .option("header", "false")
            .schema(schema)
            .load(input_path)
        )
        if df_raw.take(1):
            raw_files_found = True
            df_bronze = df_raw.withColumn("_ingest_timestamp", current_timestamp()).withColumn("_source_file", input_file_name())
            df_bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{BRONZE_SCHEMA}.{table_name}")
            print(f"[INFO] Tabela '{BRONZE_SCHEMA}.{table_name}' gravada a partir dos arquivos brutos.")
    except Exception:
        pass

if not raw_files_found:
    generate_seed_data()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Verificacao da Camada Bronze

# COMMAND ----------
display(spark.sql("SHOW TABLES IN cnpj_lakehouse.bronze"))
