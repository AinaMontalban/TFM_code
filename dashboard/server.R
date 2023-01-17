#
# This is the server logic of a Shiny web application. You can run the
# application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#

# install.packages("RPostgreSQL")
# install.packages("RPostgres")
library(DBI)
library(shiny)
library(glue)
library(ggplot2)
library(data.table)
library(dplyr)
library(tidyr)
library(treemap)
library(stringr)
# Connect to the DB
#conn <- dbConnect(
#  RPostgres::Postgres(),
#  dbname = "ngs_quality_db",
#  host = "localhost",
#  port = "5432",
#  user = "amontalban",
#  password = "jhcb89!"
#)

conn <- dbConnect(
  RPostgres::Postgres(),
  dbname = "ngs_quality_db",
  host = "localhost",
  port = "5432",
  user = "postgres",
  password = "postgres"
)



# Define server logic required to draw a histogram
shinyServer(function(input, output) {
  
  
#  output$title <- renderUI({
#    HTML(paste0("Activity for year: ", input$year))
#  })
  
  output$title<- renderUI({
    tags$a(paste0("Activity for year: ", input$year),style = "font-size: 40px; color: black;")
  })
  
  #############################################################################
  #################### Tab 1: General metrics
  #############################################################################
  # Q1: Total number of runs sequenced during the year
  output$num_runs <- renderValueBox({
    query <- DBI::sqlInterpolate(conn, "SELECT COUNT(run_id) 
                                        FROM sequencing_qc 
                                        WHERE date_part('year', time_id) = ?year;", 
                                        year=input$year) 
    num_runs <- dbGetQuery(conn, query)
    valueBox(
      num_runs, "Total number of runs sequenced",
      color = "blue"
    )
  })
  # Q2: Total number of samples analysed during the year
  output$num_samples <- renderValueBox({
    query <- DBI::sqlInterpolate(conn, "SELECT COUNT(sample_id) 
                                        FROM bioinfo_analysis_qc 
                                        WHERE date_part('year', time_id) = ?year;",
                                        year=input$year) 
    num_samples <- dbGetQuery(conn, query)
    
    valueBox(
      num_samples, "Total number of samples analysed",
      color = "blue"
    )
  })
  # Q3: Proportion of protocols performed during the specified year
  output$runprop <- renderPlot({
    query <- DBI::sqlInterpolate(conn, "SELECT rd.protocol_id, COUNT(*) 
                                        FROM sequencing_qc rs, run_dimension rd
                                        WHERE rd.run_id=rs.run_id AND date_part('year', time_id) = ?year 
                                      GROUP BY rd.protocol_id", year=input$year)
    outp <- dbGetQuery(conn, query)
    treemap(outp, index=c("protocol_id"), vSize="count", title = "Proportion of runs sequenced per protocol")
  })
  
  # Q4: Proportion of samples based on their panel performed during the specified year
    output$panelinfo <- renderPlot({
      query <- DBI::sqlInterpolate(conn, "SELECT sa.panel_id, sa.time_id, count(*) as num_samples
                                          FROM bioinfo_analysis_qc sa, time_dimension t 
                                          WHERE t.time_id=sa.time_id AND date_part('year', sa.time_id) = ?year 
                                          GROUP BY sa.panel_id, sa.time_id;", year=input$year)
      panel_res <- dbGetQuery(conn, query)
      
      data <- as.data.frame(panel_res)
      
      # plot the data
      ggplot(data, aes(x=as.factor(panel_id), y=as.numeric(num_samples))) + 
        geom_col(aes(fill=as.factor(panel_id))) + coord_flip() + 
        guides(fill="none") + labs(y="Number of samples", x="") + theme_minimal()+
        theme(text = element_text(size=15),
              axis.text.x = element_text(angle=90, hjust=1), axis.text.x.top = element_text(size=20))
    })
    

    #############################################################################
    #################### Tab 2: Protocol metrics
    #############################################################################
    output$protocol_qc <- renderPlot({
      
      if (input$qinstrument == "All") {
      comp_metrics <- DBI::sqlInterpolate(conn, "SELECT rs.cluster_density, rs.cluster_pf as Cluster_Passing_Filter, rs.pct_gt_30 as Pct_GT_30, 
                                                                  rs.yieldg as Yield_G, rs.pct_aligned as PCT_Aligned, rs.time_id as time
                                                    FROM sequencing_qc rs, run_dimension rd
                                                    WHERE rd.run_id=rs.run_id AND rd.protocol_id = ?protocol AND date_part('year', rs.time_id) = ?year;", 
                                                      protocol = input$protocol_name, year=input$year)
    } else {
        comp_metrics <- DBI::sqlInterpolate(conn, "SELECT rs.cluster_density, rs.cluster_pf as Cluster_Passing_Filter, rs.pct_gt_30 as Pct_GT_30, 
                                                                  rs.yieldg as Yield_G, rs.pct_aligned as PCT_Aligned, rs.time_id as time
                                                  FROM sequencing_qc rs, run_dimension rd
                                                  WHERE rd.run_id=rs.run_id AND rd.protocol_id = ?protocol 
                                                  AND rs.instrument_id = ?instrument AND date_part('year', rs.time_id) = ?year;", 
                                                  protocol = input$protocol_name, instrument=input$qinstrument, year=input$year)
      }
      
      df <- dbGetQuery(conn, comp_metrics)
      df$time <- as.Date(df$time)
      ggplot2::ggplot(data=df, aes_string(x="time", y=input$qmetric)) + geom_line() +
        geom_point() + theme_classic() +
        labs(y=gsub("_"," ",input$qmetric), x="Time") +
        theme(axis.text = element_text(face = "bold", size=12), axis.title = element_text(face="bold", size=15))
      
    })
    
    #############################################################################
    #################### Tab 3: Run metrics
    #############################################################################
    run_metrics <- eventReactive(input$submit, {
      
      query <- DBI::sqlInterpolate(conn, "SELECT rs.cluster_density, cast(rs.cluster_pf as decimal(10,2)) as cluster_passing_filter, cast(rs.pct_gt_30 as decimal(10,2)) as pct_gt_30, 
                                cast(rs.yieldg as decimal(10,2)) as yield_g, rd.protocol_id, rs.instrument_id 
                                FROM sequencing_qc rs, run_dimension rd
                                   WHERE rd.run_id=rs.run_id AND rd.run_id = ?run;", run = input$run)
      outp <- dbGetQuery(conn, query)
      return(outp)
    })
    
    output$run_metrics <- renderTable({
      df <- run_metrics()
      t_df <- transpose(df)
      colnames(t_df) <- "Value"
      rownames(t_df) <- str_to_title(gsub("_", " ", colnames(df)))
      ret <- as.data.frame(t_df)}, 
      rownames = TRUE, width = "100%", striped = TRUE, digits = 3)  
    
    run_protocol_metrics <- eventReactive(input$submit, {
      
      get_protocol_query <- DBI::sqlInterpolate(conn, "SELECT rd.protocol_id FROM sequencing_qc rs, run_dimension rd
                                   WHERE rd.run_id=rs.run_id AND rd.run_id = ?run;", run = input$run)
      protocol_input <- dbGetQuery(conn, get_protocol_query)$protocol
      
      comp_metrics <- DBI::sqlInterpolate(conn, "SELECT rs.cluster_density as cluster_density, rs.cluster_pf as cluster_passing_filter, rs.pct_gt_30, rs.yieldg, 
                            rs.pct_aligned 
                                  FROM sequencing_qc rs, run_dimension rd
                                   WHERE rd.run_id=rs.run_id AND rd.protocol_id = ?protocol", protocol = protocol_input)
      df <- dbGetQuery(conn, comp_metrics)
      
      return(df)
    })
    
    output$comparative_analysis <- renderPlot({
      df <- as.data.frame(run_protocol_metrics())
      my_run_metric <- as.data.frame(run_metrics())[as.character(input$metric_comp)][,1]
      
      ggplot2::ggplot(data=df, aes_string(x=input$metric_comp)) + geom_density( fill="lightblue", alpha=0.3) +
        geom_vline(aes_string(xintercept=my_run_metric), color="red") + theme_minimal()
    })
    
    
    output$seqmetrics <- renderPlot({
      query <- DBI::sqlInterpolate(conn, "SELECT * 
                                           FROM run_sequencing 
                                          WHERE instrument_id = ?instrument;", instrument = input$instrument)
      seq_metrics <- dbGetQuery(conn, query)
      df <- as.data.frame(seq_metrics)
      ggplot2::ggplot(data=df, aes_string(x="time_id", y=input$metric)) + geom_line() + geom_point()
    })
  
    #############################################################################
    #################### Tab 4: Sample metrics
    #############################################################################  
      sample_metrics <- eventReactive(input$submit_sample, {
      query <- DBI::sqlInterpolate(conn, "SELECT sample_id, run_id, panel_id, TO_CHAR(time_id,'dd-mm-yyyy') as time, total_sequences, 
                        cast(read_length as decimal(10,2)) as read_length,
                                          CONCAT((pct_gc), '%') as gc_content, CONCAT((cast(pct_duplicates as decimal(10,2))), '%') as Duplicates, 
                                          cov38x as Coverage_at_38x, mean_cov as mean_coverage
                                          FROM bioinfo_analysis_qc
                                          WHERE sample_id = ?sample;", 
                                          sample = input$sample)
      outp <- dbGetQuery(conn, query)
      return(outp)
    })
    
    output$sam_metrics <- renderTable({
      df <- sample_metrics()
      t_df <- transpose(df)
      colnames(t_df) <- "Value"
      rownames(t_df) <- gsub("_", " ", colnames(df))
      rownames(t_df) <- str_to_title(rownames(t_df))
      ret <- as.data.frame(t_df)}, 
      rownames = TRUE, width = "100%", striped = TRUE, digits = 3)  
    
    sample_protocol_metrics <- eventReactive(input$submit_sample, {
      
      get_protocol_query <- DBI::sqlInterpolate(conn, "SELECT panel_id FROM bioinfo_analysis_qc
                                   WHERE sample_id = ?sample;", sample = input$sample)
      panel_input <- dbGetQuery(conn, get_protocol_query)$panel_id
      
      comp_metrics <- DBI::sqlInterpolate(conn, "SELECT * FROM bioinfo_analysis_qc
                                   WHERE panel_id = ?panel", panel = panel_input)
      df <- dbGetQuery(conn, comp_metrics)
      
      return(df)
    })
    
    
    output$comparative_analysis_sam <- renderPlot({
      df <- as.data.frame(sample_protocol_metrics())
      my_sam_metric <- as.data.frame(sample_metrics())[as.character(input$metric_comp_sam)][,1]
      
      
      ggplot2::ggplot(data=df, aes_string(x=input$metric_comp_sam)) + geom_density( fill="lightblue", alpha=0.3) +
        geom_vline(aes_string(xintercept=my_sam_metric), color="red") + theme_minimal()
    })
    #############################################################################
    #################### Tab 5: Instrument and Reagent metrics
    #############################################################################   
    output$instrument_plot <- renderPlot({
      query <- paste0("SELECT  f.cluster_density, f.cluster_pf as Cluster_Passing_Filter, f.pct_gt_30 as Pct_GT_30, 
                                                                  f.yieldg as Yield_G, f.pct_aligned as PCT_Aligned, f.time_id, f.instrument_id, d.instrument_name
                      FROM sequencing_qc f, instrument_dimension d WHERE f.instrument_id=d.instrument_id AND f.instrument_id IN (", 
                      paste(paste0("'",input$instrument_list, "'"), collapse = ","), ")", "AND date_part('year', f.time_id) = ", input$year)
     # query <- DBI::sqlInterpolate(conn, "SELECT * 
      #                                     FROM run_sequencing 
      #                                    WHERE instrument_id IN (?instrument);", instrument = input$instrument_list)
     seq_metrics <- dbGetQuery(conn, query)
      df <- as.data.frame(seq_metrics)
      df$time_id <- as.Date(df$time_id)
      ggplot2::ggplot(data=df, aes_string(x="time_id", y=input$metric_comp_instr, fill="instrument_name", color="instrument_name")) + geom_line() + 
        geom_point() + theme_minimal() + labs(x="Time")
    })

})

