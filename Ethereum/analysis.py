import csv
import sys
import pandas as pd
import igraph
from igraph import *
from multiprocessing.pool import ThreadPool, Pool

def stats_single(from_ts, step):
    el_path = 'data/processed/el_{}_{}.csv'.format(from_ts, from_ts + step)
    el = pd.read_csv(el_path, header = None)

    map_path = 'data/processed/map_{}_{}.csv'.format(from_ts, from_ts + step)
    mapping = pd.read_csv(map_path, header = None)
    
    el.columns = ['from', 'to', 'val', 'ts']
    el = el.drop(['val', 'ts'], axis=1)

    multi_g = Graph.DataFrame(el, directed = True)
    simple_g = multi_g.simplify(multiple=True, loops=True, combine_edges=None)
    print('finish reading')

    simple_g = simple_g.subgraph(simple_g.vs.select(_degree_gt=10))

    num_nodes = simple_g.vcount()
    num_edges = simple_g.ecount()
    dense = simple_g.density(loops = False)
    reci = simple_g.reciprocity()
    asso = simple_g.assortativity_degree(directed=False)
    motif = multi_g.motifs_randesu_no()
    trans = simple_g.transitivity_undirected()
    return from_ts, num_nodes, num_edges, dense, reci, asso, motif, trans

def stats_multi(from_timestamp, to_timestamp, step):
    with ThreadPool(20) as pool:
        input_args = []
        while from_timestamp <= to_timestamp: 
            input_args.append((from_timestamp, step))
            from_timestamp += step
        print('number of networks to generate', len(input_args))

        results = pool.starmap(stats_single, input_args)
        with open('result.csv', 'w') as f:
            for r in results:
                writer = csv.writer(f)
                writer.writerow(r)

# 1519702425
stats_multi(1629169207,1632798007,86400) # 2017