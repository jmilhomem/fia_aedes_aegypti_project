library(shiny)
setwd("/home/labdata/projeto_final/R")

dataBairros <- read.csv("/home/labdata/projeto_final/dados/bairros_pe.csv",sep="|")
dataBairros[,"bairro_estacao"] <- paste0(dataBairros[,"Bairro"]," - ",dataBairros[,"Estação"])
dataBairros <-dataBairros[order(dataBairros$bairro_estacao),] 

shinyUI(navbarPage(title = "Data2U"
                   ,tabPanel(title = "Processamento por parâmtros"

                             ,h4("Informe os parâmetros para predição:")
                             ,br()
                             ,selectInput("bairro","Bairro - Estação"
                                          ,choices = dataBairros[,"bairro_estacao"]
                                          ,width = 500)
                             ,column(4,sliderInput("populacao", "População:", min = 1, max = 1000000, value = 1000, step = 100, width = "100%"))
                             ,column(8,dateInput("data", "Data:", format = "dd/mm/yyyy", value = "2017-03-01", width = 110))
                             ,column(12,sliderInput("precipitacao", "Precipitação:", min = 0.01, max = 100, value = 1, step = 0.01, width = "32%"))
                             ,column(4,sliderInput("tempMaxima", "Temp. Máxima:", min = 1, max = 50, value = 18, step = 0.10, width = "100%"))
                             ,column(4,sliderInput("tempMinima", "Temp. Mínima:", min = 1, max = 50, value = 38, step = 0.10, width = "100%"))
                             ,column(4,sliderInput("tempCompMedia", "Temp. Comp. Média:", min = 1, max = 50, value = 28, step = 0.10, width = "100%"))
                             ,br()
                             ,column(12,actionButton("createDF", label = "Processar"))
                             ,column(12,textOutput("process"))
                             ,column(12,a("View log",target="_blank",href="http://horse:8088/cluster"))
                             
                             
                   )
                   ,tabPanel(title = "Processamento por arquivos"
                            ,column(12,fileInput('originFile', 'Selecione o arquivo de bairros'
                                       ,accept=c('text/csv','text/comma-separated-values,text/plain','.csv')
                                       ,buttonLabel = "Bairros"
                                       ,width = "50%"))
                            ,column(12,tableOutput("tabBairros"))
                            ,column(12,fileInput('originFilePrecTemp', 'Selecione o arquivo de estações'
                                        ,accept=c('text/csv','text/comma-separated-values,text/plain','.csv')
                                        ,buttonLabel = "Estações"
                                        ,width = "50%"))
                            ,column(12,tableOutput("tabPrecTemp"))
                            
                            ,br()
                            ,column(12,textInput("hdfsPath","path HDFS",value = "/user/labdata/", width="25%"))
                            ,br()
                            
                            ,column(12,actionButton("fileCopy", label = "Processar"))
                            ,column(12,textOutput("process2"))
                            ,column(12,a("View Hadoop log",target="_blank",href="http://horse:8088/cluster"))
                            
                   )
                   ,tabPanel(title = "Upload de arquivos para HDFS"
                             ,column(12,textInput("hdfsPath2","path HDFS",value = "/user/labdata/", width="25%"))
                             ,br()
                             ,column(12,fileInput('originFileSystem', 'Selecione o arquivo '
                                                  ,accept=c('text/csv','text/comma-separated-values,text/plain','.csv')
                                                  ,buttonLabel = "File"
                                                  ,width = "50%"))
                             ,column(12,textOutput("process3"))
                             
                   )
    )
)
