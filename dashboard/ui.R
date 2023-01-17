#
# This is the user-interface definition of a Shiny web application. You can
# run the application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#

library(shiny)
library(shinydashboard)
#install.packages("shinydashboard")

ui <- dashboardPage(
  skin = "green",
  ########################################################
  ##### Header
  ########################################################
  dashboardHeader(title = "NGS Quality Dashboard"),
  
  ########################################################
  ##### Sidebar
  ########################################################
  dashboardSidebar(
    sidebarMenu(
      selectInput('year', 'Year', choices = c("2016", "2017", "2018", "2019", "2020", "2021", "2022"), selected = "2022"),
       menuItem("Dashboard", tabName = "Dashboard")   ,
      menuItem("Protocols", tabName = "Protocol")   ,
      menuItem("Runs", tabName = "Runs")   ,
      menuItem("Samples", tabName = "Samples")   ,
      menuItem("Instruments", tabName = "Instruments")    )
  
  ),
  ########################################################
  ##### Body
  ########################################################
  dashboardBody(
    ########################################################
    ##### Tab 1: General Activity
    ########################################################
    tabItems(
      tabItem(tabName = "Dashboard",
              htmlOutput("title"),
              fluidRow(
              infoBoxOutput("num_runs", width=3),
              infoBoxOutput("num_samples", width = 3)),
              fluidRow(
                box(title="Runs", status="primary", plotOutput("runprop")),
                box(title="Samples", status="primary", 
                  plotOutput("panelinfo"))
                
              )
              ),
      ########################################################
      ##### Tab 2: By protocol metrics
      ########################################################
      tabItem(tabName = "Protocol", 
                     fluidRow(
                       box(
                         selectInput(inputId="protocol_name", label="Choose Protocol:",
                                     choices=c("Exome" = "EX",
                                               "Custom Panel GM" = "CUSTOM",
                                               "Hereditary Cancer" = "TC"), selected = 1),
                         selectInput(inputId="qmetric", label="Quality metric:",
                                     choices=c("Cluster Density" = "cluster_density",
                                               "Clusters PF" = "cluster_passing_filter",
                                               "> Q30" = "pct_gt_30",
                                               "Yield G" = "yield_g",
                                               "% Aligned" = "pct_aligned"), selected = 1),
                         selectInput(inputId="qinstrument", label="Instrument:",
                                     choices=c("All" = "All",
                                       "MiSeq 0" = "M00289",
                                               "MiSeq 1" = "M04372",
                                               "MiSeq 2" = "M06751",
                                               "NextSeq 550" = "NB551293",
                                               "NextSeq550Dx RUO" = "NDX550505_RUO",
                                               "NextSeq 2000" = "VH00757"
                                              ), selected = 1)
                        
                       ), box(
                         plotOutput("protocol_qc"))
                     )
              ,
                       fluidRow(
                        )),
      ########################################################
      ##### Tab 3: By run metrics
      ########################################################
      tabItem(tabName = "Runs",
              fluidRow(
              box(
                title = "Search Run:", solidHeader = TRUE,
                textInput("run", "Run:"),
                actionButton(inputId = "submit", label = "Search"))),
              fluidRow(
              box(tableOutput("run_metrics"), title = "Run Metrics:"),
              box(  
                  selectInput(inputId="metric_comp", label="Quality metric:",
                              choices=c("Cluster Density" = "cluster_density",
                                        "Clusters Passing Filter" = "cluster_passing_filter",
                                        "> Q30" = "pct_gt_30",
                                        "Yield G" = "yield_g",
                                        "% Aligned" = "pct_aligned"), selected = 1),
                  plotOutput("comparative_analysis"), title = "Run Comparative Analysis:"),
                )),
      ########################################################
      ##### Tab 4: By sample metrics
      ########################################################
      tabItem(tabName = "Samples",
              fluidRow(
              box(title = "Search Sample:", solidHeader = TRUE,
        textInput("sample", "Sample:"),
        actionButton(inputId = "submit_sample", label = "Search"))),
        fluidRow(
        box(tableOutput("sam_metrics"), title = "Sample Metrics:"),
        box(  
        selectInput(inputId="metric_comp_sam", label="Quality metric:",
                    choices=c("Number of reads" = "total_sequences" ,
                              "Read Length" = "read_length",
                              "Coverage at 38x" = "Coverage_at_38x",
                              "Mean Coverage" = "mean_coverage","Number of reads" = "total_sequences"), selected = 1),
        plotOutput("comparative_analysis_sam"), title = "Sample Comparative Analysis:"),
    )),
      
      ########################################################
      ##### Tab 5: By instrument and reagent metrics
      ########################################################
      tabItem(tabName = "Instruments",
              box(
              checkboxGroupInput("instrument_list", "Select Instruments:", choices = c("MiSeq 0" = "M00289",
                                 "MiSeq 1" = "M04372",
                                 "MiSeq 2" = "M06751",
                                 "NextSeq 550" = "NB551293",
                                 "NextSeq550Dx RUO" = "NDX550505_RUO",
                                 "NextSeq 2000" = "VH00757"))),
              box(selectInput(inputId="metric_comp_instr", label="Quality metric:",
                              choices=c("Cluster Density" = "cluster_density",
                                        "Clusters Passing Filter" = "cluster_passing_filter",
                                        "> Q30" = "pct_gt_30",
                                        "Yield G" = "yield_g",
                                        "% Aligned" = "pct_aligned"), selected = 1),
                  plotOutput("instrument_plot"), title = "Instrument Comparison:")
    )
                        
  ))
)


# https://stackoverflow.com/questions/65517000/making-multiple-queries-to-posgresql-database-through-r-shiny-ui
