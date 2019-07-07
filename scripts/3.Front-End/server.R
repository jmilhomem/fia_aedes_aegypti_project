library(shiny)
library(stringr)

shinyServer(function(input, output) {

  # Cria os arquivo de bairros e estações a máquina
  fileCreate <- eventReactive(input$createDF, {
    dfB <- data.frame("Bairro" = substr(input$bairro, 1, str_locate(input$bairro," - ")[1]-1)
                      ,"População" =  input$populacao
                      ,"Estação" = substr(input$bairro, str_locate(input$bairro," - ")[2]+1, str_length(input$bairro)))
    write.csv2(dfB, "/home/labdata/projeto_final/dados/predict_bairros.csv")
    
    # Estações -> Criar data frame, arquivo local e enviar para hdfs
    dfE <-data.frame("codigo_estacao" = "0"
                     ,"Nome_estacao" = substr(input$bairro, str_locate(input$bairro," - ")[2]+1, str_length(input$bairro))
                     ,"Data" = format(as.Date(input$data, "%B %d %Y"), "%d/%m/%Y")
                     ,"Precipitacao" = input$precipitacao
                     ,"temp_maxima" = input$tempMaxima
                     ,"temp_minima" = input$tempMinima
                     ,"temp_comp_media" = input$tempCompMedia
    )
    write.csv2(dfE, "/home/labdata/projeto_final/dados/predict_estacoes.csv")
    
    dfE
    
  })
  
  # copia os arquivos criados (fileCreate), envia para o HDFS e executa o script Python
  executeCmd <- eventReactive(input$createDF, {

    termId = rstudioapi::terminalCreate(show = TRUE)
    
    vCmd <- paste0("hdfs dfs -rm "," /user/labdata/predict_bairros.csv","\n")
    rstudioapi::terminalSend(termId, vCmd)    
    
    vCmd <- paste0("hdfs dfs -put ","/home/labdata/projeto_final/dados/predict_bairros.csv"," /user/labdata/predict_bairros.csv","\n")
    rstudioapi::terminalSend(termId, vCmd)    
    
    vCmd <- paste0("hdfs dfs -rm "," /user/labdata/predict_estacoes.csv","\n")
    rstudioapi::terminalSend(termId, vCmd)    
    
    vCmd <- paste0("hdfs dfs -put ","/home/labdata/projeto_final/dados/predict_estacoes.csv"," /user/labdata/predict_estacoes.csv","\n")
    rstudioapi::terminalSend(termId, vCmd)            
    
    vCmd <- paste0("/home/labdata/spark-2.2.1-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode client " #cluster
                  ,"/home/labdata/projeto_final/python/3_FIA_predicao_por_bairro.py "
                  ," \n")
    termId = rstudioapi::terminalCreate(show = TRUE)
    rstudioapi::terminalSend(termId, vCmd)
    
  })
  
  # executa as funções fileCreate e executeCMD
  output$process <- renderText({
    
    fileCreate()
    
    executeCmd()
    
    print(paste0("Em processamento, acompanhe o log pelo link a seguir "))
    
  })  
  
  # Lê o arquivo de bairros no filesystem e copia para o hdfs 
  output$tabBairros <- renderTable({
    if ( is.null(input$originFile$datapath)  ) {
      return(NULL)
    }
    else {
      dfB <- read.csv(paste0(input$originFile$datapath), sep = ";")

      termId = rstudioapi::terminalCreate(show = TRUE)
      
      vCmd <- paste0("hdfs dfs -rm "," /user/labdata/predict_bairros.csv","\n")
      rstudioapi::terminalSend(termId, vCmd)    
      
      vCmd <- paste0("hdfs dfs -put ",input$originFile$datapath," /user/labdata/predict_bairros.csv","\n")
      print(paste0("datapath: ",input$originFile$datapath))
      print(paste0("vCmd: ",vCmd))
      
      rstudioapi::terminalSend(termId, vCmd)
      
      br()
      
      dfB
    }
  })
  
  # Lê o arquivo de estações no filesystem e copia para o hdfs 
  output$tabPrecTemp <- renderTable({
    if ( is.null(input$originFilePrecTemp$datapath)  ) {
      return(NULL)
    }
    else {
      dfT <- read.csv(paste0(input$originFilePrecTemp$datapath), sep = ";")
      
      termId = rstudioapi::terminalCreate(show = TRUE)
      
      vCmd <- paste0("hdfs dfs -rm "," /user/labdata/predict_estacoes.csv","\n")
      rstudioapi::terminalSend(termId, vCmd)    
      
      vCmd <- paste0("hdfs dfs -put ",input$originFilePrecTemp$datapath," /user/labdata/predict_estacoes.csv","\n")
      rstudioapi::terminalSend(termId, vCmd)
      
      br()
      
      dfT
    }
  })
  
  # executa o script python
  output$process2 <- renderText({
    
    vCmd <- paste0("/home/labdata/spark-2.2.1-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode client " #cluster
                  ,"/home/labdata/projeto_final/python/3_FIA_predicao_por_bairro.py "
                  ," \n")
    termId = rstudioapi::terminalCreate(show = TRUE)
    rstudioapi::terminalSend(termId, vCmd)
    
    if (input$fileCopy > 0) {
      print(paste0("Em processamento, acompanhe o log pelo link a seguir "))
    }
  })
  
  output$process3 <- renderText({
    if ( is.null(input$originFileSystem$datapath)  ) {
      return(NULL)
    }
    else {
      
      termId = rstudioapi::terminalCreate(show = TRUE)
      
      vCmd1 <- paste0("hdfs dfs -rm ","/user/labdata/",input$originFileSystem$name,"\n")
      rstudioapi::terminalSend(termId, vCmd1)
      
      vCmd2 <- paste0("hdfs dfs -put ",input$originFileSystem$datapath," /user/labdata/", input$originFileSystem$name,"\n")
      rstudioapi::terminalSend(termId, vCmd2)
      
    }    
  })
  
})