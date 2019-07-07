# FIA - Aedes Aegypti Project


O projeto foi desenvolvido em grupo por:

| Membros | Linked-in |
| ------ | ------ |
| Felipe Higa | https://www.linkedin.com/in/felipehiga/ |
| Jessika Milhomem | https://www.linkedin.com/in/jmilhomem/ |
| Lyzbeth Cronembold | https://www.linkedin.com/in/lyzbethcronembold/ |
| Marco Conti | https://www.linkedin.com/in/marco-conti-br/ |

## 1. Overview do Projeto
Esse trabalho foi desenvolvido durante a especialização em Big Data Analytics realizada na [FIA](https://fia.com.br/) pelo grupo descrito acima.
O objetivo do projeto é criar uma arquitetura de dados que atenda as necessidades de negócios da Prefeitura do Município de  Recife quanto as campanhas de prevenção  à contaminação pelo mosquito Aedes Aegypti nos próximos 2 meses.

**Principal análise preditiva desejada**:
Listagem da quantidade de pessoas e relação de bairros que a prefeitura deverá fazer as campanha de prevenção para maximizar os recursos financeiros disponíveis.

## 2. Bases de dados e sources:
Arquivos:
* **casos-chikungunya2016.csv**:  Arquivo que contém casos de pessoas infectadas pelo Chikungunya no ano de 2016. Essa base foi extraída do site http://dados.recife.pe.gov.br/ro/dataset
* **casos-dengue2016.csv**:  Arquivo que contém casos de pessoas infectadas pela dengue no ano de 2016. Essa base foi extraída do site http://dados.recife.pe.gov.br/ro/dataset
* **casos-zika_2016.csv** : Arquivo que contém casos de pessoas infectadas pelo Zika no ano de 2016. Essa base foi extraída do site http://dados.recife.pe.gov.br/ro/dataset
* **Bairros_pe.csv**: Arquivo contém a relação entre bairros e estações meteorológicas e a população dos bairros. Esta base foi extraída do site do IBGE
* **Todas as estações 2015 a 2017.csv**: Arquivo contém informações de temperaturas ocorridas nas estações. Esta base foi extraída do site (www.inmet.gov.br/)

## 3. Arquitetura proposta: 
![alt text](/images/arquitetura_proposta.png "")

## 4. Arquitetura implementada: 
![alt text](/images/arquitetura_implementada.png "")
 
## 5. Scripts criados:
**Jupyter em Python**: 
Análise inicial exploratória utilizando Python, onde foram feitos estudos da base e estudos de modelos de dados
Script:
* **Modelo_PYTHON.ipynb**: Estudo/ Exploração dos dados compostos nas bases extraídas. Além disso, foram realizados estudos e testes dos modelos de dados estatísticos para consideração do modelo de dados final: Boosting.
* **Modelo_PYTHON.html**: Export do arquivo jupyter em formato HTML.

**Jupyter com PySpark**: 
Scripts criados com o tratamento de dados dos arquivos importados, modelagem de dados considerando o modelo de dados estudado em "Modelo_Python.ipynb e por fim, script em produção, responsável por utilizar modelo de dados, considerar bases de dados geradas pelos usuários de negócios e efetuar previsão dos dados. Scripts:
* **1_FIA_Tratamento_base_ML_v6.py**: Script utilizado para tratar a base inicial (2015) para estudo e criação de modelo final. Ele foi criado para ser executado uma única vez.
* **Machine Learning_v2.ipynb**: Jupyter onde o Data Scientist fez o estudo e criação de modelo. Assim que concluído, foi salvo no HDFS para utilização posterior.
* **3_FIA_predicao_por_bairro.py**: Script produtivo criado para ler dados de base que será utilizada para previsão do modelo (quantidade de casos de pessoas picadas pelo Aedes Aegypti por bairro e semana). Ao final da previsão, essa base prevista é inserida (merge) na tabela usada para criação do modelo no Hive. Essa tabela será lida pelo Impala, que está sendo lido no PowerBI.

## 6. Front end
**Scripts em Shiny**: Tela para leitura de parâmetros ou arquivos, que serão armazenados no HDFS, para serem lidos posteriormente pelo Spark para efetuar a predição. Também é possível enviar arquivos para o HDFS. Scripts:
* **ui.R**: Componentes gráficos da tela(frames, campos, botões, links)
* **server.R**: Processamentos. 

## 7. PowerBI: Criado um arquivo com reportes resultantes de análise
* **Visão - Casos Aedes Aegypt.pbix**: Esse arquivo contém reportes com análise exploratória utilizando as bases unificadas, a qual por fim será utilizada para criação do modelo de dados. E por fim, tem um relatório que possibilita análise comparativa entre os dados realizados de 2016 e o período desejado previsto.

---
# FIA - Aedes Aegypti Project

The project was developed in group by:

| Members | Linked-in |
| ------ | ------ |
| Felipe Higa | https://www.linkedin.com/in/felipehiga/ |
| Jessika Milhomem | https://www.linkedin.com/in/jmilhomem/ |
| Lyzbeth Cronembold | https://www.linkedin.com/in/lyzbethcronembold/ |
| Marco Conti | https://www.linkedin.com/in/marco-conti-br/ |

## 1. Objective of the project:
This posgraduate work was done by the group listed above during the Big Data Analytics Specialization at [FIA](https://fia.com.br/) and the purpose was to develop a data model, which should be responsible to predict how many infected people might have in the neighborhoods of the city of Recife in Brazil. 
The purpose of this model is to support the Recife's town hall with data for them to plan how to allocate the budget and implement the Zika's, Dengue's and/or Chikungunya's prevention campaigns as best as possible together with the population.

## 2. Datasets and sources:
Datasets:
* **casos-chikungunya2016.csv**: File that contains cases of infected people by Chikungunya in 2016. That dataset was extracted from http://dados.recife.pe.gov.br/ro/dataset
* **casos-dengue2016.csv**: File that contains cases of infected people by Dengue in 2016. That dataset was extracted from  http://dados.recife.pe.gov.br/ro/dataset
* **casos-zika_2016.csv** : File that contains cases of infected people by Zika in 2016. That dataset was extracted from http://dados.recife.pe.gov.br/ro/dataset
* **Bairros_pe.csv**: File that contains the neighborhoods and its stations, besides the number of neighborhood's population. It was extracted from IBGE's site.
* **Todas as estações 2015 a 2017.csv**: File that contains the historical temperature information that occured on the stations. It was extracted from www.inmet.gov.br

## 3. Architecture proposed: 
![alt text](/images/arquitetura_proposta.png "")

## 4. Architecture implemented: 
![alt text](/images/arquitetura_implementada.png "")

## 5. Scripts created:
**Python's Jupyter**: 
Initial exploratory analyse created in Python and tested the data models.
Script:
* **Modelo_PYTHON.ipynb**: Exploration of data done on the datasets extracted. Besides, the datamodels were tested until define the best final data model: Boosting.
* **Modelo_PYTHON.html**: Export in HTML format of the previous script explained.

**PySpark's Jupyter Scripts**: 
Scripts created to do the treatments of imported files, data modeling considering the data models studied in "Modelo_Python.ipynb" file and finally the production script, which import the final datamodel created in Pyspark, new datasets created and passed by users to do the prediction as needed. Scripts:
* **1_FIA_Tratamento_base_ML_v6.py**: Script used to treat the initial datasets mentioned before (2016) to create the final datamodel. It was created to be executed once.
* **Machine Learning_v2.ipynb**: Jupyter which the "Data Scientist" created to do the creation of the final data model, which once created, was saved at HDFS for future utilization.
* **3_FIA_predicao_por_bairro.py**: Productive Script created to read the dataset which will be used to do the prediction (quantity of people infected by Aedes Aegypti by neighborhood). At the end, this predicted dataset will be stored (appended) to the final table into Hive, which already has the data used to create the data model. This table will be read by Impala, which will be used as a data source by PowerBI.

## 6. Front end
**Shiny's Scripts**: Frond-end used by users to pass the parameters or files, which will be stored into HDFS, and read by Spark afterward to do the prediction. Scripts:
* **ui.R**: Graphics Components (frames, attributes, bottons, links)
* **server.R**: Processing. 

## 7. PowerBI: File created with results of analysis done (report).
* **Visão - Casos Aedes Aegypt.pbix**: File that contains reports with exploration analysis using the unified datasets, which will be used to create the final data model. At the end, was created a final report that enables us to do a comparative analyse of the actual data of 2016 and the prediction period desired.
