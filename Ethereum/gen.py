import csv
import sys
from multiprocessing.pool import ThreadPool, Pool

def set_csv_limit():
    max_size = sys.maxsize
    while True:
        try:
            csv.field_size_limit(max_size)
            break
        except OverflowError:
            max_size = int(max_size / 10)

def generate(input_path, output_path, from_timestamp, to_timestamp, step):
    while from_timestamp <= to_timestamp:
        generate_single(input_path, output_path, from_timestamp, step)
        from_timestamp += step

def generate_multi(input_path, output_path, from_timestamp, to_timestamp, step):
    with ThreadPool(20) as pool:
        input_args = []
        while from_timestamp <= to_timestamp: 
            input_args.append((input_path, output_path, from_timestamp, step , to_timestamp))
            from_timestamp += step
        print('number of networks to generate', len(input_args))

        results = pool.starmap(generate_single, input_args)
        print(results)
        print('done')

def generate_single(input_path, output_path, from_timestamp, step, final_ts):
    print('processing', from_timestamp)
    set_csv_limit()
    n = 0
    nodes = {}
    to_timestamp = from_timestamp + step
    input_path = input_path + "/part_{}_{}.csv".format(from_timestamp, to_timestamp)
    edge_list_path = '{}/el_{}_{}.csv'.format(output_path, from_timestamp, to_timestamp)
    node_mappings_path = '{}/map_{}_{}.csv'.format(output_path, from_timestamp, to_timestamp)
    
    in_file = open(input_path, 'r', newline ='') 
    edge_list_file = open(edge_list_path, 'w', newline ='') 
    node_mappings_file = open(node_mappings_path, 'w', newline ='') 
    
    reader = csv.reader(in_file)
    el_writer = csv.writer(edge_list_file)
    map_writer = csv.writer(node_mappings_file)

    for row in reader:
        if nodes.get(row[5]) is None:
            nodes.update({row[5] : n})
            n += 1
        if nodes.get(row[6]) is None:
            nodes.update({row[6]: n})
            n += 1
    
        el_writer.writerow([nodes[row[5]], nodes[row[6]], int(row[7]) * float(1.0) / (10**18), row[11]])  

    for k, v in nodes.items():
        map_writer.writerow([k, v])
    
    in_file.close()
    edge_list_file.close()
    node_mappings_file.close()
    print('finsihed', from_timestamp)
    return edge_list_path, node_mappings_path

if __name__ == '__main__':
    generate_multi(
        'data/partitioned', 
        'data/processed',
        1632798007, 
        1632798007,
        86400
    )