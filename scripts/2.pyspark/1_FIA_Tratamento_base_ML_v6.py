from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as func
#import pyspark
import pandas as pd

import re



#Função que lista colunas que tenham algum nulo
def lista_colunas_com_algum_nulo(df):
    dicionario_nulos = df.select([func.count(func.when(func.col(coluna).isNull(), coluna)).alias(coluna) for coluna in df.columns]).collect()[0].asDict()
    colunas_com_nulo = [{nome_coluna:qtd_nulos} for nome_coluna, qtd_nulos in dicionario_nulos.items() if qtd_nulos > 0]
    return colunas_com_nulo




#Funcao para fazer a concatenacao usando a logica de outer union.
def harmoniza_schemas_e_combina(df_esquerda, df_direita):
    """
     Se um campo do df_x esta faltando no df_y, então é adicionado o campo no df_y com valor nulo. 
     """
  
    # lista tipos decolunas do dataframe, e ordena os objetos por nome, tipo de dados e nulos.
    tipos_df_esquerda = {f.name: f.dataType for f in df_esquerda.schema}
    tipos_df_direita = {f.name: f.dataType for f in df_direita.schema}
    colunas_df_esquerda = set((f.name, f.dataType, f.nullable) for f in df_esquerda.schema)
    colunas_df_direita = set((f.name, f.dataType, f.nullable) for f in df_direita.schema)

    # Primeiro verifica se os tipos de dados do df da esquerda sao os mesmos tipos de dados do df da direita
    for e_coluna, e_tipo, e_nulos in colunas_df_esquerda.difference(colunas_df_direita):
        if e_coluna in tipos_df_direita:
            d_tipo = tipos_df_direita[e_coluna]

        df_direita = df_direita.withColumn(e_coluna, func.lit(None).cast(e_tipo))

    # Em seguida, verifica se os tipos de dados do df da direita sao os mesmos tipos de dados do df da esquerda
    for d_coluna, d_tipo, d_nulos in colunas_df_direita.difference(colunas_df_esquerda):
        if d_coluna in tipos_df_esquerda:
            e_tipo = tipos_df_esquerda[d_coluna]

        df_esquerda = df_esquerda.withColumn(d_coluna, func.lit(None).cast(d_tipo))    

    # Lista a ordem de colunas dos dataframes de forma sequencial (set)
    df_esquerda = df_esquerda.select(df_direita.columns)

    #Retorna um union dos dois datasets
    return df_esquerda.union(df_direita)



#Funcao que executa sequencial de tratativa
def trata_base_para_criar_machine_learning():

    ####################################################################################################################
    #                           1. IMPORTAÇÃO DAS BASES
    ####################################################################################################################    

    """ --------------------  Importa dados de bairros - Dados de 2016   --------------------"""    

    #Importa dados de bairros - Dados de 2016
    df_bairros_pe_import = sqlContext.read.format('csv').options(header='true', delimiter='|', quote= "\"", inferSchema='true').load('/user/labdata/bairros_pe.csv')    

    #usado para testar a execução no Cluster, atraves de leitura dos logs
    """for c in df_bairros_pe_import.collect():
        print("Bairro:", c["Bairro"], ", População:", c["População"], ", Estação:", c["Estação"]) """

    #renomeia colunas com caracteres especiais
    df_bairros_pe_original = df_bairros_pe_import.toDF(*(c.replace('Bairro', 'bairro').replace('População', 'populacao').replace('Estação', 'estacao') for c in df_bairros_pe_import.columns))    
    #df_bairros_pe_original.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    
      
    #Cria tabela temporaria de bairros
    df_bairros_pe_original.registerTempTable("tb_df_bairros_pe")
    df_bairros_pe = sqlContext.sql("""
                             SELECT *
                            FROM    tb_df_bairros_pe 
                            """)

    #salva arquivo como tabela no hive
    hive_context.sql("CREATE TABLE IF NOT EXISTS tb_bairros_pe as select * from tb_df_bairros_pe")
    hive_context.sql("insert overwrite table tb_bairros_pe select * from tb_df_bairros_pe")


    # --------------------  Importa dados de Todas as estações 2015 a 2017   --------------------#

    #Importa dados de Todas as estacoes de 2015 a 2017
    df_estacoes_import = sqlContext.read.format('csv').options(header='true', delimiter='|', quote= "\"", inferSchema='true').load('/user/labdata/Todas_estacoes_2015_a_2017.csv')     

    #renomeia colunas com caracteres especiais
    df_estacoes_original = df_estacoes_import.toDF(*(c.replace('ção', 'cao') for c in df_estacoes_import.columns))
    df_estacoes_original = df_estacoes_original.toDF(*(c.replace(' da ', '_') for c in df_estacoes_original.columns))
    df_estacoes_original = df_estacoes_original.toDF(*(c.replace('Temp. Comp. Média', 'temp_comp_media') for c in df_estacoes_original.columns))
    df_estacoes_original = df_estacoes_original.toDF(*(c.replace('Temp. Máxima', 'temp_maxima') for c in df_estacoes_original.columns))
    df_estacoes_original = df_estacoes_original.toDF(*(c.replace('Código', 'codigo') for c in df_estacoes_original.columns))
    df_estacoes_original = df_estacoes_original.toDF(*(c.replace('Temp. Mínima', 'temp_minima') for c in df_estacoes_original.columns))       

    #Criando uma coluna calculada concatenando estacao e data para chave
    df_estacoes_original.registerTempTable("temp_df_estacoes_original")
    df_estacoes = sqlContext.sql("""
                           SELECT codigo_estacao
                                  , Nome_estacao
                                  --, concat(Data, ' ', '00:00:00') as data_sem_tratamento
                                  , regexp_replace(cast(to_timestamp(concat(Data, ' ', '00:00:00'), 'dd/MM/yyyy HH:mm:ss') as string), '-', '/') as Data
                                  , Key
                                  , Precipitacao
                                  , temp_maxima
                                  , temp_minima
                                  , temp_comp_media
                                  , concat(Nome_estacao,'', Data) as chave_base
                          FROM    temp_df_estacoes_original 
                          """)     
    #Criando uma coluna calculada concatenando estacao e data para chave
    df_estacoes.registerTempTable("tb_df_estacoes")
    df_estacoes = sqlContext.sql("""
                           SELECT *
                          FROM    tb_df_estacoes 
                          """)       

    # Cria tabela no Hive
    hive_context.sql("CREATE TABLE IF NOT EXISTS tb_estacoes as select * from tb_df_estacoes")
    hive_context.sql("insert overwrite table tb_estacoes select * from tb_df_estacoes")



    # --------------------  Importa dados de chikungunya - Dados de 2016   --------------------#  


    df_chikungunya_import = sqlContext.read.format('csv').options(header='true', delimiter=';', quote= "\"", inferSchema='true').load('/user/labdata/dadosrecifegov_casos-chikungunya2016.csv')       

    # No df_zika há uma coluna "ano_notificacao" diferente das outras bases de dados mas com a mesma informação.
    df_chikungunya_import = df_chikungunya_import.toDF(*(c.replace('dt_diagnostico_sintoma', 'dt_diagnostico_sintoma_old') for c in df_chikungunya_import.columns))       

    # Para o estudo há a necessidade de criar uma coluna destacando os casos de zika.
    df_chikungunya_import.registerTempTable("temp_df_chikungunya2016")
    df_chikungunya = sqlContext.sql("""
                           SELECT *
                                  , 0 as zika
                                  --dt_diagnostico_sintoma_old
                                  , regexp_replace(cast(dt_diagnostico_sintoma_old as string), '-', '/') as dt_diagnostico_sintoma
                                  , 'chikungunya' as source_dados
                          FROM    temp_df_chikungunya2016
                          """)  

    # Cria tabela no Hive
    df_chikungunya.registerTempTable("tmp_df_chikungunya")
    hive_context.sql("CREATE TABLE IF NOT EXISTS tb_chikungunya as select * from tmp_df_chikungunya")
    hive_context.sql("insert overwrite table tb_chikungunya select * from tmp_df_chikungunya")
    
    
    # --------------------  Importa dados de zika - Dados de 2016   --------------------#


    ##Importa dados de zika - Dados de 2016
    df_zika_import = sqlContext.read.format('csv').options(header='true', delimiter=';', quote= "\"", inferSchema='true').load('/user/labdata/dadosrecifegov_casos-zika_2016.csv')       

    # No df_zika há uma coluna "ano_notificacao" diferente das outras bases de dados mas com a mesma informação.
    df_zika = df_zika_import.toDF(*(c.replace('ano_notificacao', 'notificacao_ano') for c in df_zika_import.columns))       

    # Para o estudo há a necessidade de criar uma coluna destacando os casos de zika.
    df_zika.registerTempTable("temp_df_zika2016")     
    df_zika = sqlContext.sql("""SELECT *
                                      , 1 AS zika
                                      --dt_diagnostico_sintoma
                                      --, regexp_replace(cast(to_timestamp(concat(dt_diagnostico_sintoma_old, ' ', '00:00:00'), 'dd/MM/yyyy HH:mm:ss') as string), '-', '/') as Data
                                      , 'zika' as source_dados
                                 from temp_df_zika2016
                           """)  

    # Cria tabela no Hive
    df_zika.registerTempTable("tmp_df_zika")
    hive_context.sql("CREATE TABLE IF NOT EXISTS tb_zika as select * from tmp_df_zika")
    hive_context.sql("insert overwrite table tb_zika select * from tmp_df_zika")
    
    
    # --------------------  Importa dados de Dengue - Dados de 2016   --------------------#  

    # Importa dados de dengue - Dados de 2016
    df_dengue_import = sqlContext.read.format('csv').options(header='true', delimiter=';', quote= "\"", inferSchema='true').load('/user/labdata/dadosrecifegov_casos-dengue2016.csv')       

    #Renomeia coluna de data de diagnostico
    df_dengue_import = df_dengue_import.toDF(*(c.replace('dt_diagnostico_sintoma', 'dt_diagnostico_sintoma_old') for c in df_dengue_import.columns))

    #Trata coluna de data de diagnostico
    df_dengue_import.registerTempTable("temp_df_dengue2016")
    df_dengue = sqlContext.sql("""
                           SELECT *
                                  , 0 as zika
                                  -- dt_diagnostico_sintoma_old
                                  , regexp_replace(cast(to_timestamp(concat(dt_diagnostico_sintoma_old, ' ', '00:00:00'), 'dd-MM-yyyy HH:mm:ss') as string), '-', '/') as dt_diagnostico_sintoma
                                  , 'dengue' as source_dados
                          FROM    temp_df_dengue2016
                          """) 
    
    # Cria tabela no Hive
    df_dengue.registerTempTable("tmp_df_dengue")
    hive_context.sql("CREATE TABLE IF NOT EXISTS tb_dengue as select * from tmp_df_dengue")
    hive_context.sql("insert overwrite table tb_dengue select * from tmp_df_dengue")    
                          
    print("\nAs bases de dados apresentam diferentes quantidades de features.\n")         
    print("Chikungunya -> ", "qtd de registros: ", df_chikungunya.count(), ", qtd de features: ", len(df_chikungunya.columns))
    print("Dengue -> ", "qtd de registros: ", df_dengue.count(), ", qtd de features: ", len(df_dengue.columns))
    print("Zika -> ", "qtd de registros: ", df_zika.count(), ", qtd de features: ", len(df_zika.columns))
    print("Bairros -> ", "qtd de registros: ", df_bairros_pe.count(), ", qtd de features: ", len(df_bairros_pe.columns))
    print("Estações -> ", "qtd de registros: ", df_estacoes.count(), ", qtd de features: ", len(df_estacoes.columns))


    ####################################################################################################################
    #                           2. UNIFICAÇÃO DAS BASES
    ####################################################################################################################  

    # --------------------   Unifica as 3 bases   --------------------#  

    #Utiliza funcao para unificar os dados
    df_pre_union = harmoniza_schemas_e_combina(df_dengue, df_chikungunya)      

    #Utiliza funcao para unificar os dados, agora considerando o segundo dataframe
    df_geral = harmoniza_schemas_e_combina(df_pre_union, df_zika)      


    print("\nAs bases de dados apresentam diferentes quantidades de features.\n")      

    print("Bairros -> ", "qtd de registros: ", df_bairros_pe.count(), ", qtd de features: ", len(df_bairros_pe.columns))
    print("Estações -> ", "qtd de registros: ", df_estacoes.count(), ", qtd de features: ", len(df_estacoes.columns))
    print("Chikungunya -> ", "qtd de registros: ", df_chikungunya.count(), ", qtd de features: ", len(df_chikungunya.columns))
    print("Dengue -> ", "qtd de registros: ", df_dengue.count(), ", qtd de features: ", len(df_dengue.columns))
    print("Zika -> ", "qtd de registros: ", df_zika.count(), ", qtd de features: ", len(df_zika.columns))
    print("Pre-Union (Dengue e Chikungunya) -> ", "qtd de registros: ", df_pre_union.count(), ", qtd de features: ", len(df_pre_union.columns))
    print("Union Final (Dengue + Chikungunya + Zika) -> ", "qtd de registros: ", df_geral.count(), ", qtd de features: ", len(df_geral.columns))      

    
    # --------------------   Cria tabela no Hive  --------------------# 
    df_geral.registerTempTable("temp_df_base_modelos_unificadas_2016")
    hive_context.sql("CREATE TABLE IF NOT EXISTS tb_base_unificada_2016_para_modelos as select * from temp_df_base_modelos_unificadas_2016")
    hive_context.sql("insert overwrite table tb_base_unificada_2016_para_modelos select * from temp_df_base_modelos_unificadas_2016")

    
    ####################################################################################################################
    #                           3. TRATAMENTO DE BASES
    ####################################################################################################################  


    # --------------------   Relaciona tabelas   --------------------#
    df_geral_2 = sqlContext.sql("""
                           SELECT temp_df_base_modelos_unificadas_2016.*
                                  , tb_bairros_pe.populacao
                                  , tb_bairros_pe.estacao
                                  , concat(tb_bairros_pe.estacao, '', temp_df_base_modelos_unificadas_2016.dt_diagnostico_sintoma) as chave_base
                                  
                             FROM temp_df_base_modelos_unificadas_2016
                                  LEFT JOIN tb_bairros_pe
                                         ON temp_df_base_modelos_unificadas_2016.no_bairro_residencia = tb_bairros_pe.bairro
                           """)      



    df_geral_2.registerTempTable("temp_df_base_para_modelos_unificadas_2016")
    df_geral_3 = sqlContext.sql("""
                           SELECT *
                             FROM temp_df_base_para_modelos_unificadas_2016 
                                  LEFT JOIN tb_estacoes
                                         ON temp_df_base_para_modelos_unificadas_2016.estacao = tb_estacoes.Nome_estacao
                                        AND temp_df_base_para_modelos_unificadas_2016.dt_diagnostico_sintoma = tb_estacoes.Data
                           """)      

    print("Bases unificadas -> ", "qtd de registros: ", df_geral_3.count(), ", qtd de features: ", len(df_geral_3.columns))  


    # --------------------   Tratamentos para serem aplicados de cleasing na base   --------------------#  

    #Remove variáveis que não interessam para o modelo
    excluir_colunas=['nu_notificacao', 'tp_notificacao', 'co_cid', 'co_uf_residencia', 'co_municipio_residencia', 'co_regional_residencia',
            'co_distrito_residencia', 'co_bairro_residencia', 'co_logradouro_residencia', 'co_geo_campo_1', 'co_geo_campo_2',
            'nome_referencia', 'nu_cep', 'tp_zona_residencia', 'co_pais_residencia', 'dt_investigacao', 'co_cbo_ocupacao',
            'febre', 'mialgia', 'cefaleia', 'exantema', 'vomito','nausea', 'dor_costas', 'conjutivite', 'artrite', 'artralgia',
             'petequia_n', 'leucopenia', 'laco', 'dor_retro', 'diabetes','hematolog', 'hepatopat', 'renal', 'hipertensao', 'acido_pept',
             'auto_imune', 'dt_chil_s1', 'dt_chil_s2', 'dt_prnt', 'res_chiks1','res_chiks2', 'resul_prnt', 'dt_coleta_isolamento',
             'tp_result_isolamento', 'dt_coleta_rtpcr', 'tp_result_rtpcr','tp_sorotipo', 'st_ocorreu_hospitalizacao', 'dt_internacao',
             'co_uf_hospital', 'co_municipio_hospital', 'co_unidade_hospital','nu_ddd_hospital', 'nu_telefone_hospital', 'tp_autoctone_residencia',
             'co_uf_infeccao', 'co_pais_infeccao', 'co_municipio_infeccao','co_distrito_infeccao', 'co_bairro_infeccao', 'no_bairro_infeccao',
             'tp_classificacao_final', 'tp_criterio_confirmacao','st_doenca_trabalho', 'clinc_chik', 'tp_evolucao_caso', 'dt_obito',
             'dt_encerramento', 'ds_obs', 'tp_sistema', 'dt_digitacao','tp_fluxo_retorno', 'st_fluxo_retorno_recebido','ds_identificador_registro',
            'Tp_result_NS1','alrm_abdom','alrm_hemat','alrm_hepat','alrm_hipot','alrm_letar','alrm_liq','alrm_plaq','alrm_sang',
            'alrm_vom','cns_sus','complica','dt_alrm','dt_coleta_NS1','dt_transf_dm','dt_transf_rm','dt_transf_rs','dt_transf_se',
            'dt_transf_sm', 'dt_transf_us','epistaxe','evidencia','gengivo','hematura','hematura','id_rg_residencia','dt_coleta_exame',
            'laco_n','mani_hemor','metro','nm_logradouro_residencia','nu_cep_residencia','nu_lote_horizontal','nu_lote_vertical',
            'petequias','plaq_menor','plaq_menor','plasmatico','sangram','st_importado','st_vincula','tp_duplicidade','tp_result_exame',
            'tp_result_histopatologia','tp_result_imunohistoquimica','tp_suspeita','dt_diagnostico_sintoma',
            'chave','Código da estação','Estacao_y','id_regional', 'Data','co_municipio_notificacao','co_uf_notificacao',
            'co_unidade_notificacao','notificacao_ano','nu_idade','nome_logradouro_residencia','ds_semana_sintoma',
            'dt_nascimento','dt_notificacao','tp_escolaridade','tp_gestante','tp_raca_cor','tp_sexo', 'Key', 'Nome_estacao',
             'chave_base', 'codigo_estacao', 'dt_diagnostico_sintoma_old', 'zika']           

    df_geral_4 = df_geral_3.select([column for column in df_geral_3.columns if column not in excluir_colunas])           


    #verifica quantidade de registros e features
    print("Dataframe sem colunas -> ", "qtd de registros: ", df_geral_4.count(), ", qtd de features: ", len(df_geral_4.columns))           


    df_geral_4.registerTempTable("temp_df_base_tratada")
    df_geral_5 = sqlContext.sql("""
                        SELECT ds_semana_notificacao
                               , UPPER(coalesce(no_bairro_residencia, 'SEM_INFORMACAO')) as no_bairro_residencia
                               , UPPER(source_dados) AS source_dados
                               , case when populacao is not null then populacao
                                      else (
                                         select avg(populacao)
                                         from  temp_df_base_tratada
                                         ) 
                                 end as populacao
                               , UPPER(coalesce(CASE WHEN estacao = 'CABROBÓ' THEN 'CABROBO' ELSE estacao END, 'SEM_INFORMACAO')) as estacao
                               , Precipitacao as Precipitacao_ori
                               , case when Precipitacao is not null then cast(regexp_replace(Precipitacao,',','.') as float)
                                      else (
                                         select avg(cast(regexp_replace(Precipitacao,',','.') as float))
                                         from  temp_df_base_tratada
                                         ) 
                                 end as precipitacao
                               , temp_maxima as temp_maxima_ori
                               , case when temp_maxima is not null then cast(regexp_replace(temp_maxima,',','.') as float)
                                      else (
                                         select avg(cast(regexp_replace(temp_maxima,',','.') as float))
                                         from  temp_df_base_tratada
                                         ) 
                                 end as temp_maxima
                               , temp_minima as temp_minima_ori
                               , case when temp_minima is not null then cast(regexp_replace(temp_minima,',','.') as float)
                                      else (
                                         select avg(cast(regexp_replace(temp_minima,',','.') as float))
                                         from  temp_df_base_tratada
                                         ) 
                                 end as temp_minima
                                 , temp_comp_media as temp_comp_media_ori
                               , case when temp_comp_media is not null then cast(regexp_replace(temp_comp_media,',','.') as float)
                                      else (
                                         select avg(cast(regexp_replace(temp_comp_media,',','.') as float))
                                         from  temp_df_base_tratada
                                         ) 
                                 end as temp_comp_media
                           FROM temp_df_base_tratada
                        """)  

    ####################################################################################################################
    #                           4. CRIAÇÃO DE BASE PARA MODELO
    ####################################################################################################################  

    df_geral_5.registerTempTable("temp_df_base_para_modelo")
    df_geral_base = sqlContext.sql("""
                            SELECT no_bairro_residencia
                                   , ds_semana_notificacao
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
                                 SELECT no_bairro_residencia
                                        , estacao
                                        , cast(count(*) as int) as qtd
                                  FROM temp_df_base_para_modelo
                                  group by 1, 2
                                 """)
    df_geral_base_pivot = df_geral_aux_bairro_estacao.groupBy("no_bairro_residencia").pivot("estacao").agg(func.sum("qtd")/func.count("qtd"))      


    #renomeia colunas de estacoes
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('ARCOVERDE', 'estacao_arcoverde') for c in df_geral_base_pivot.columns))
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('CABROBO', 'estacao_cabrobo') for c in df_geral_base_pivot.columns))
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('GARANHUNS', 'estacao_garanhuns') for c in df_geral_base_pivot.columns))
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('OURICURI', 'estacao_ouricuri') for c in df_geral_base_pivot.columns))
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('PETROLINA', 'estacao_petrolina') for c in df_geral_base_pivot.columns))
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('RECIFE', 'estacao_recife') for c in df_geral_base_pivot.columns))
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('SEM_INFORMACAO', 'estacao_seminfo') for c in df_geral_base_pivot.columns))
    df_geral_base_pivot = df_geral_base_pivot.toDF(*(c.replace('SURUBIM', 'estacao_surubim') for c in df_geral_base_pivot.columns))      


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
                                   , 'original' as ds_tipo_base
                              FROM temp_df_base_para_modelo_bairros
                                   LEFT JOIN temp_df_pivot
                                          ON temp_df_base_para_modelo_bairros.no_bairro_residencia = temp_df_pivot.no_bairro_residencia
                             """)  

    print("Base final -> ", "qtd de registros: ", df_geral_base_final.count(), ", qtd de features: ", len(df_geral_base_final.columns))  

    # --------------------   Cria tabela no Hive  --------------------# 
    df_geral_base_final.registerTempTable("temp_df_base_final")
    hive_context.sql("CREATE TABLE IF NOT EXISTS tb_base_final_para_ml_doencas as select * from temp_df_base_final")
    hive_context.sql("insert overwrite table tb_base_final_para_ml_doencas select * from temp_df_base_final")
    


if __name__ == "__main__":

    conf = SparkConf().setAppName("bairros_infectados")#.set("spark.hadoop.validateOutputSpecs", "true");
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    hive_context = HiveContext(sc)    

    #executa sequencia de tratativas para base e machine learning
    trata_base_para_criar_machine_learning()    