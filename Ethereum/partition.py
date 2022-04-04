import csv
import sys
import pandas as pd
import networkx as nx

def partition(input_file_path, output_path, from_timestamp, step):
    csv.field_size_limit(sys.maxsize)
    in_file = open(input_file_path, 'r', newline ='')
    reader = csv.reader(in_file)
    outputs = {}
    cur_ts = from_timestamp
    
    next(reader)
    i = 0
    for row in reader:
        i += 1
        if i % 10000 == 0:
            print('procsessing row', i)
        
        start = from_timestamp + step * ((int(row[11]) - from_timestamp) // step)
        end = start + step
        filename = 'part_{}_{}.csv'.format(start, end)
        if filename not in outputs:
            fout = open(output_path + '/' + filename, 'a')
            writer = csv.writer(fout)
            outputs[filename] = fout, writer
        outputs[filename][1].writerow(row)  
    
    for fout, _ in outputs.values():
        fout.close()
    
if __name__ == '__main__':
    partition(
        'origin/transactions.csv', 
        'partitioned',
        1629255607,
        86400,
    )