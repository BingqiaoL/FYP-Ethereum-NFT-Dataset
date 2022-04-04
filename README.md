# Scope
Sample dataset of the Ethereum and NFT transaction data.

# Data
* [Ethereum](#Ethereum)
* [NFT](#NFT)

# Ethereum
  - original/
    - transactions_sample.csv: transaction data crawled using Ethereum-ETL https://ethereum-etl.readthedocs.io/en/latest/
  - partitioned/
    - part_sample_1.csv and part_sample_2.csv: daily-basis transaction data partitioned using partition.py
    - el_sample.csv and map_sample.csv: edge lists and maps to contruct graphs

# NFT
  - original/
    - log_sample.csv: log information crawled using Ethereum-ETL 
  - processed/
    - logs_processed_sample.csv: NFT data filtered out using gen.py
    - el_sample.csv and map_sample.csv: edge lists and maps to contruct graphs
    
