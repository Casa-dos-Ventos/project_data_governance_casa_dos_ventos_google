# Data Governance Project Tables and Dashboard
This example shows all codes to create a Data Governance Pipeline using Dataplex and Big Query in Google Cloud Platform in a company of Renewable Energy in Brazil - Casa dos Ventos. This repository is based from this post on google [here](link)

The dashboard has 2 pages:
1. Data Governance of BigQuery
2. Dataplex Analysis

The dashboard uses 3 tables (1,2,3) and a view (4) that can be created using the following scripts:
1. [bigquery_tables_analysis](./Get_data_bigquery/bigquery_tables_analysis)
2. [bigquery_views_analysis](./Get_data_bigquery/bigquery_views_analysis)
3. [dataplex_assets_analysis](./Get_data_dataplex/dataplex_assets_analysis)
4. [check_bq_datasets_in_dataplex](./Get_data_bigquery/sql_views)

### Description of tables and views
bigquery_tables_analysis - Table with information about all tables in organization (snapshot of the day).

bigquery_views_analysis - Table with information about all views in organization (snapshot of the day).

dataplex_assets_analysis - Table with information about all assets in organization's dataplex (snapshot of the day).

check_bq_datasets_in_dataplex - View with join between datasets and assets in dataplex to analyse new datasets not mapped in dataplex.

### Creating a copy of dashboard in Data Studio
Create a copy of [this](https://lookerstudio.google.com/u/0/reporting/3f9e3b2e-8dd3-44b1-b8ea-0bfc572c6563/preview) Dashboard.

After clicking on the Copy button, you will find a message asking you to choose a new data source. Select the data sources created.

Click on create report. Rename the report (dashboard) to a name of your choice.

### About Casa dos Ventos
To learn about Casa dos Ventos, visit our website [here](https://casadosventos.com.br/en) 

<h2>Contact me for questions and new ideas </h3>
    <p>
        <a href="https://www.linkedin.com/in/claudio-c%C3%A9sar-506961164/"><img src="https://img.shields.io/badge/Claudio%20Cesar-blue?style=plastic&amp;labelColor=blue&amp;logo=LinkedIn&amp;link=https://www.linkedin.com/in/claudio-c%C3%A9sar-506961164/" alt="LinkedIn Badge"></a> 
   </p>
