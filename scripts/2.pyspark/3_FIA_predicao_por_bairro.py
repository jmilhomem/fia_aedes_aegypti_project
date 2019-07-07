from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.regression import GBTRegressor
import pyspark.sql.functions as func
import pandas as pd

import re

#funcao para importar os arquivos
def import_csv_files(p_arquivo, p_delimitador):
    df_file = sqlContext.read.format('csv').options(header='true', delimiter= p_delimitador, quote= "\"", inferSchema='true').load('/user/labdata/' + p_arquivo)
    return df_file


#Funcao que executa sequencial de tratativa e roda modelo de machine learning
def trata_base_e_roda_machine_learning():
	
    ####################################################################################################################
    #                           1. IMPORTAÇÃO DAS BASES DE PREDICAO
    ####################################################################################################################        

    """ --------------------  Importa dados de bairros passados pelo usuario para predição  --------------------"""    

    #importa arquivo criado
    df_bairros_pe_import = import_csv_files('predict_bairros.csv', ';')    

    #renomeia colunas com caracteres especiais
    df_bairros_pe_original = df_bairros_pe_import.toDF(*(c.replace('Bairro', 'bairro').replace('População', 'populacao').replace('Estação', 'estacao') for c in df_bairros_pe_import.columns))    

    #Cria tabela temporaria de bairros para tratamento posterior
    df_bairros_pe_original.registerTempTable("tb_df_bairros_pe")
    df_bairros_pe = sqlContext.sql("""
                             SELECT *
                            FROM    tb_df_bairros_pe 
                            """)    
    
    """ --------------------  Importa dados de estações passados pelo usuario para predição  --------------------"""    

    #importa arquivo criado
    df_estacoes_import = import_csv_files('predict_estacoes.csv', ';')    

    #renomeia colunas com caracteres especiais
    df_estacoes_original = df_estacoes_import.toDF(*(c.replace('ção', 'cao') for c in df_estacoes_import.columns))
    df_estacoes_original = df_estacoes_original.toDF(*(c.replace(' da ', '_') for c in df_estacoes_original.columns))
    df_estacoes_original = df_estacoes_original.toDF(*(c.replace('Temp. Comp. Média', 'temp_comp_media') for c in df_estacoes_original.columns))
    df_estacoes_original = df_estacoes_original.toDF(*(c.replace('Temp. Máxima', 'temp_maxima') for c in df_estacoes_original.columns))
    df_estacoes_original = df_estacoes_original.toDF(*(c.replace('Código', 'codigo') for c in df_estacoes_original.columns))
    df_estacoes_original = df_estacoes_original.toDF(*(c.replace('Temp. Mínima', 'temp_minima') for c in df_estacoes_original.columns))    
    
    #Criando uma coluna calculada concatenando estacao e data para chave
    df_estacoes_original.registerTempTable("tb_estacoes_original")
    df_estacoes = sqlContext.sql("""
                         SELECT codigo_estacao
                                , Nome_estacao as estacao
                                , to_timestamp(concat(Data, ' ', '00:00:00'), 'dd/MM/yyyy HH:mm:ss') as Data
                                , case when cast(length(weekofyear(to_timestamp(concat(Data, ' ', '00:00:00'), 'dd/MM/yyyy HH:mm:ss'))) as string) = '1' 
                                       then concat(cast(year(to_timestamp(concat(Data, ' ', '00:00:00'), 'dd/MM/yyyy HH:mm:ss')) as string), concat('0', cast(weekofyear(to_timestamp(concat(Data, ' ', '00:00:00'), 'dd/MM/yyyy HH:mm:ss')) as string)))
                                       else concat(cast(year(to_timestamp(concat(Data, ' ', '00:00:00'), 'dd/MM/yyyy HH:mm:ss')) as string), cast(weekofyear(to_timestamp(concat(Data, ' ', '00:00:00'), 'dd/MM/yyyy HH:mm:ss')) as string))
                                  end as ano_semana
                                , Precipitacao as precipitacao
                                , temp_maxima
                                , temp_minima
                                , temp_comp_media
                        FROM    tb_estacoes_original 
                        """)    

    #Criando uma tabela em memoria para cruzamento e tratamento de bases posteriormente
    df_estacoes.registerTempTable("tb_df_estacoes")
    df_estacoes = sqlContext.sql("""
                           SELECT *
                          FROM    tb_df_estacoes 
                          """)         

    ####################################################################################################################
    #                           2. TRATAMENTO DA BASE - HARMONIZACAO DOS DADOS
    ####################################################################################################################    

    # --------------------   Relaciona tabelas   --------------------#
    df_merge_bases = sqlContext.sql("""
                           SELECT tb_df_estacoes.*
                                  , tb_df_bairros_pe.bairro
                                  , tb_df_bairros_pe.populacao
                                 -- , tb_df_bairros_pe.estacao
                                 -- , concat(tb_df_bairros_pe.estacao, '', temp_df_base_modelos_unificadas_2016.dt_diagnostico_sintoma) as chave_base    

                             FROM tb_df_estacoes
                                  LEFT JOIN tb_df_bairros_pe
                                         ON tb_df_estacoes.estacao = tb_df_bairros_pe.estacao
                           """)    

    # --------------------  Faz pre-tratamento dos dados (HARMONIZACAO DOS DADOS) --------------------#
    df_merge_bases.registerTempTable("tb_df_merge_base")
    df_base_tratada = sqlContext.sql("""
                        SELECT ano_semana
                               , UPPER(coalesce(bairro, 'SEM_INFORMACAO')) as bairro
                               , case when populacao is not null then populacao
                                      else (
                                         select avg(populacao)
                                         from  tb_df_merge_base
                                         ) 
                                 end as populacao
                               , UPPER(coalesce(CASE WHEN estacao = 'CABROBÓ' THEN 'CABROBO' ELSE estacao END, 'SEM_INFORMACAO')) as estacao
                               , precipitacao as precipitacao_ori
                               , case when precipitacao is not null then cast(regexp_replace(precipitacao,',','.') as float)
                                      else (
                                         select avg(cast(regexp_replace(precipitacao,',','.') as float))
                                         from  tb_df_merge_base
                                         ) 
                                 end as precipitacao
                               --, temp_maxima as temp_maxima_ori
                               , case when temp_maxima is not null then cast(regexp_replace(temp_maxima,',','.') as float)
                                      else (
                                         select avg(cast(regexp_replace(temp_maxima,',','.') as float))
                                         from  tb_df_merge_base
                                         ) 
                                 end as temp_maxima
                               --, temp_minima as temp_minima_ori
                               , case when temp_minima is not null then cast(regexp_replace(temp_minima,',','.') as float)
                                      else (
                                         select avg(cast(regexp_replace(temp_minima,',','.') as float))
                                         from  tb_df_merge_base
                                         ) 
                                 end as temp_minima
                                 , temp_comp_media as temp_comp_media_ori
                               , case when temp_comp_media is not null then cast(regexp_replace(temp_comp_media,',','.') as float)
                                      else (
                                         select avg(cast(regexp_replace(temp_comp_media,',','.') as float))
                                         from  tb_df_merge_base
                                         ) 
                                 end as temp_comp_media
                           FROM tb_df_merge_base
                        """)      
    

    ####################################################################################################################
    #                           3. TRATAMENTO DA BASE - TRATAMENTO DE MISSINGS
    ####################################################################################################################    

    df_base_tratada.registerTempTable("temp_df_base_para_modelo")
    df_geral_base = sqlContext.sql("""
                                SELECT bairro
                                       , ano_semana
                                       , count(*) as resposta
                                       , avg(populacao) as populacao
                                     --  , avg(precipitacao) as precipitacao
                                       , avg(temp_maxima) as temp_maxima
                                       , avg(temp_minima) as temp_minima
                                       , avg(temp_comp_media) as temp_comp_media
                                  FROM temp_df_base_para_modelo
                                  group by 1, 2
                               """)          

    df_geral_aux_bairro_estacao = sqlContext.sql("""
                                 SELECT bairro
                                        , estacao
                                        , cast(count(*) as int) as qtd
                                  FROM temp_df_base_para_modelo
                                  group by 1, 2
                                 """)
    df_geral_base_pivot = df_geral_aux_bairro_estacao.groupBy("bairro").pivot("estacao").agg(func.sum("qtd")/func.count("qtd"))          
    

    #renomeia colunas de estacoes
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('ARCOVERDE', 'estacao_arcoverde') for c in df_geral_base_pivot.columns))
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('CABROBO', 'estacao_cabrobo') for c in df_geral_base_pivot.columns))
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('GARANHUNS', 'estacao_garanhuns') for c in df_geral_base_pivot.columns))
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('OURICURI', 'estacao_ouricuri') for c in df_geral_base_pivot.columns))
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('PETROLINA', 'estacao_petrolina') for c in df_geral_base_pivot.columns))
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('RECIFE', 'estacao_recife') for c in df_geral_base_pivot.columns))
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('SEM_INFORMACAO', 'estacao_seminfo') for c in df_geral_base_pivot.columns))
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('SURUBIM', 'estacao_surubim') for c in df_geral_base_pivot.columns))      
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('TRIUMFO', 'estacao_triumfo') for c in df_geral_base_pivot.columns))          
    

    #identifica colunas que precisam ser criadas
    colunas_necessarias = set(['estacao_recife', 'estacao_ouricuri', 'estacao_cabrobo', 'estacao_petrolina', 'estacao_seminfo', 'estacao_garanhuns', 'estacao_arcoverde','estacao_surubim','estacao_triumfo'])
    colunas_existentes = set(df_geral_base_pivot.schema.names)
    colunas_inserir = list(colunas_necessarias.difference(colunas_existentes))    
    

    #cria colunas inexistentes e necessarias para modelo
    if colunas_inserir != []:
    	for variavel in colunas_inserir:
    		df_geral_base_pivot = df_geral_base_pivot.withColumn(variavel, func.lit(0))    
    

    #cria tabela de pivot de estacoes
    df_geral_base_pivot.registerTempTable("temp_df_pivot")
    df_geral_base_pivot_analise = sqlContext.sql("""
                                 SELECT *
                                  FROM temp_df_pivot
                                 """)              

    #cria tabela final de bairros e estacoes
    df_geral_base.registerTempTable("temp_df_base_para_modelo_bairros")
    df_geral_base_final = sqlContext.sql("""
                             SELECT temp_df_base_para_modelo_bairros.*
                                   , case when estacao_arcoverde is null then 0 else estacao_arcoverde end as estacao_arcoverde
                                   , case when estacao_cabrobo is null then 0 else estacao_cabrobo end as estacao_cabrobo
                                   , case when estacao_garanhuns is null then 0 else estacao_garanhuns end as estacao_garanhuns
                                   , case when estacao_ouricuri is null then 0 else estacao_ouricuri end as estacao_ouricuri
                                   , case when estacao_petrolina is null then 0 else estacao_petrolina end as estacao_petrolina
                                   , case when estacao_recife is null then 0 else estacao_recife end as estacao_recife
                                   , case when estacao_seminfo is null then 0 else estacao_seminfo end as estacao_seminfo
                                   , case when estacao_surubim is null then 0 else estacao_surubim end as estacao_surubim
                                   , case when estacao_triumfo is null then 0 else estacao_triumfo end as estacao_triumfo
                              FROM temp_df_base_para_modelo_bairros
                                   LEFT JOIN temp_df_pivot
                                          ON temp_df_base_para_modelo_bairros.bairro = temp_df_pivot.bairro
                             """)      

    #cria vetor para modelo
    assemblerInputs = ["populacao","temp_maxima","temp_minima","temp_comp_media","estacao_arcoverde","estacao_cabrobo","estacao_garanhuns","estacao_ouricuri","estacao_petrolina","estacao_recife","estacao_seminfo","estacao_surubim","estacao_triumfo"]
    assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
    stages = [assembler]        

    pipeline = Pipeline(stages=stages)
    pipelineModel = pipeline.fit(df_geral_base_final)        

    
    df_pre_previsao = pipelineModel.transform(df_geral_base_final)    
    
    ####################################################################################################################
    #                           4. IMPORTAÇÃO DO MODELO SALVO
    ####################################################################################################################    

    modelo = PipelineModel.load("/user/labdata/model/")    
    

    ####################################################################################################################
    #                           4. PREDICAO DO MODELO
    ####################################################################################################################        

    df_base = modelo.transform(df_pre_previsao)    
    

    ####################################################################################################################
    #                           5. SALVANDO DADOS NA BASE FINAL NO HIVE
    ####################################################################################################################    

    # --------------------   Cria tabela no Hive  --------------------# 
    df_base.registerTempTable("temp_df_base_final")
    df_modelo_previsto = sqlContext.sql("""
                                 SELECT 
                                       no_bairro_residencia
                                       , ds_semana_notificacao
                                       , resposta
                                       , populacao
                                       , temp_maxima
                                       , temp_minima
                                       , temp_comp_media
                                       , estacao_arcoverde
                                       , estacao_cabrobo
                                       , estacao_garanhuns
                                       , estacao_ouricuri
                                       , estacao_petrolina
                                       , estacao_recife
                                       , estacao_seminfo
                                       , estacao_surubim
                                       , ds_tipo_base
                                  FROM tb_base_final_para_ml_doencas
                                 UNION
                                SELECT 
                                       bairro
                                       , ano_semana
                                       , prediction as resposta
                                       , populacao
                                       , temp_maxima
                                       , temp_minima
                                       , temp_comp_media
                                       , estacao_arcoverde
                                       , estacao_cabrobo
                                       , estacao_garanhuns
                                       , estacao_ouricuri
                                       , estacao_petrolina
                                       , estacao_recife
                                       , estacao_seminfo
                                       , estacao_surubim
                                       , 'previsto' as ds_tipo_base 
                                  FROM temp_df_base_final
                                 """)    
    
    df_modelo_previsto.registerTempTable("temp_df_base_final")
    hive_context.sql("CREATE TABLE IF NOT EXISTS tb_base_final_para_ml_doencas as select * from temp_df_base_final")
    hive_context.sql("insert overwrite table tb_base_final_para_ml_doencas select * from temp_df_base_final")
    
if __name__ == "__main__":

    conf = SparkConf().setAppName("predicao_infectados_por_bairro")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    hive_context = HiveContext(sc)    

    #executa sequencia de tratativas para base e machine learning
    trata_base_e_roda_machine_learning()    
