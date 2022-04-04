# Scope
Sample dataset of the Ethereum and NFT transaction data.

# Data
* [Ethereum](#Ethereum)
* [NFT](#NFT)

# Ethereum
Ethereum /:
  - original /
    - transactions_sample.csv: is the transaction data crawled using Ethereum-ETL https://ethereum-etl.readthedocs.io/en/latest/
  - partitioned /
    - part_sample_1.csv and part_sample_2.csv: are daily-basis data partitioned using partition.py
    - el_sample.csv and map_sample.csv: edge lists and maps to contruct graphs

# NFT
NFT /: 
  - original /
    - log_sample.csv: is the log information crawled using Ethereum-ETL 
  - processed /
    - logs_processed_sample.csv: NFT data filtered out using gen.py
    - el_sample.csv and map_sample.csv: edge lists and maps to contruct graphs
    
