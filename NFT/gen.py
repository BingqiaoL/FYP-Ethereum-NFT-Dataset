import os
import pandas as pd 
import csv
from web3 import Web3
import json
from hexbytes import HexBytes

my_provider = Web3.IPCProvider('jsonrpc.ipc')
w3 = Web3(my_provider)

# log_index,transaction_hash,transaction_index,block_hash,block_number,address,data,topics

LOG_PATH = '/data/logs.csv'
OUT_PATH = '/data/logs_processed.csv'
TOPIC_TRANSFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
OUT_PATH_GRAPH = '/data'
HEADER = ['address', 'block_number', 'from', 'to', 'tokenId', 'transaction_hash']
 
def parse_logs():
    def split_topics(row):
        splits = row['topics'].split(',')
        fromAccount = '0x' + splits[1][26:]
        toAccount = '0x' + splits[2][26:]
        tokenId = splits[3]
        return fromAccount, toAccount, tokenId

    def process(chunk):
        filtered_logs = chunk[chunk.apply(lambda x: 
            (str(x['topics']).split(',')[0] == TOPIC_TRANSFER) and (len(str(x['topics']).split(',')) >= 4), 
        axis=1)]
        if len(filtered_logs) == 0:
            return 
        
        filtered_logs['from'], filtered_logs['to'], filtered_logs['tokenId'] = zip(*filtered_logs.apply(split_topics, axis=1))
        parsed_logs = filtered_logs[HEADER]
        with open(OUT_PATH, 'a') as f:
            parsed_logs.to_csv(f, header=False, index=False)

    i = 0
    chunksize = 2000000
    for chunk in pd.read_csv(LOG_PATH, chunksize=chunksize, header=0):
        process(chunk)
        i += chunksize
        print('finished row', i)

def parse_logs2():
    def split_topics(row):
        splits = row['topics'].split(',')
        fromAccount = '0x' + splits[1][26:]
        toAccount = '0x' + splits[2][26:]
        tokenId = splits[3]
        return fromAccount, toAccount, tokenId

    def process(chunk):
        filtered_logs = chunk[chunk.apply(lambda x: 
            (str(x['topics']).split(',')[0] == TOPIC_TRANSFER) and (len(str(x['topics']).split(',')) >= 4), 
        axis=1)]
        if len(filtered_logs) == 0:
            return 
        
        filtered_logs['from'], filtered_logs['to'], filtered_logs['tokenId'] = zip(*filtered_logs.apply(split_topics, axis=1))
        parsed_logs = filtered_logs[HEADER]
        with open(OUT_PATH, 'a') as f:
            parsed_logs.to_csv(f, header=False, index=False)

    i = 0
    chunksize = 2000000
    for chunk in pd.read_csv(LOG_PATH, chunksize=chunksize, header=0):
        process(chunk)
        i += chunksize
        print('finished row', i)


def convert_to_graph():
    # create map (addr -> addrId)
    logs = pd.read_csv(OUT_PATH, names=HEADER)
    all_addr = pd.concat([logs['to'], logs['from']], axis=0).to_frame()
    all_addr.columns = ['addr']
    all_addr.drop_duplicates('addr', inplace=True)
    all_addr['addrId'] = pd.Categorical(all_addr['addr']).codes
    all_addr = all_addr.set_index('addr')

    logs['toId'] = logs['to'].map(all_addr['addrId'])
    logs['fromId'] = logs['from'].map(all_addr['addrId'])

    logs[['fromId', 'toId', 'tokenId', 'address', 'transaction_hash', 'block_number']].to_csv(os.path.join(OUT_PATH_GRAPH, 'el.csv'), index=False)
    all_addr.to_csv(os.path.join(OUT_PATH_GRAPH, 'map.csv'), index=True)


data = '0x0000000000000000000000000000000000000000000000000ba8478cab540000'
topics = [HexBytes('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'), 
    HexBytes('0x000000000000000000000000274f3c32c90517975e29dfc209a23f315c1e5fc7'),
    HexBytes('0x000000000000000000000000614d7d2ff69e40b1191360db583bac1576eee852')]

tx = '0x32ebeca9a4dba39df814ea8beeb516a3b6b8c720d12ca3f10ad385b00324a0a9'    
log = w3.eth.getTransactionReceipt(tx).logs[1]
log.__dict__['data'] = data
log.__dict__['topics'] = topics
print('log')
print(log)

abi_json = json.load(open('abi.json'))
contract = w3.eth.contract(address='0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D', abi=abi_json)
print('-----')
print(contract.events.Transfer().processLog(log))
