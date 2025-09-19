import csv
import os
import sys


dataset = 'wikidata'
dataset_name = 'tkgl_'+dataset
dataset_name2 = 'tkgl-'+dataset

if 'gdelt' in dataset:
    ts_size = 15
elif 'icews14' in dataset or 'icews18' in dataset: 
    ts_size = 24
else:
    ts_size = 1


if 'smallpedia' or 'wikidata' in dataset:
    rel_mapping_index = 1
    node_mapping_index = 0
else:
    rel_mapping_index = 0
    node_mapping_index = 0

print("Current sys.path:", sys.path)

# File paths
edgelist_path = os.path.join(sys.path[0], dataset_name, dataset_name2+'_edgelist.csv')
rel_mapping_path = os.path.join(sys.path[0],dataset_name, 'rel_mapping.csv')
node_mapping_path = os.path.join(sys.path[0],dataset_name, 'node_mapping.csv')


type_of_output = 'tgbid'  # or 'string' Type of output to generate

include_inverse_flags = False  # Set to True if you want to include inverse relations
split_train_val_test_flag = True

if type_of_output == 'tgbid':
    node_index = 1 + node_mapping_index
    rel_index = 0 + rel_mapping_index
    output_string_name = 'tgbid_edgelist.txt'
    if split_train_val_test_flag:
        output_string_name_train = 'tgbid_edgelist_train.txt'
        output_string_name_val = 'tgbid_edgelist_val.txt'
        output_string_name_test = 'tgbid_edgelist_test.txt'
elif type_of_output == 'string':
    node_index = 2 + node_mapping_index
    rel_index = 1 + rel_mapping_index
    output_string_name = 'string_edgelist.txt'
    if split_train_val_test_flag:
        output_string_name_train = 'string_edgelist_train.txt'
        output_string_name_val = 'string_edgelist_val.txt'
        output_string_name_test = 'string_edgelist_test.txt'
if include_inverse_flags:
    # ruledataset = RuleDataset(name=dataset_name2, threshold=1, large_data_hardcode_flag=False)
    output_string_name = 'incl_inverse_' + output_string_name
    if split_train_val_test_flag:
        output_string_name_train = 'incl_inverse_' + output_string_name_train
        output_string_name_val = 'incl_inverse_' + output_string_name_val
        output_string_name_test = 'incl_inverse_' + output_string_name_test



output_path = os.path.join(sys.path[0],dataset_name, output_string_name)

output_path_train = os.path.join(sys.path[0],dataset_name, output_string_name_train)
output_path_val = os.path.join(sys.path[0],dataset_name, output_string_name_val)
output_path_test = os.path.join(sys.path[0],dataset_name, output_string_name_test)
if not os.path.exists(output_path_train):
    open(output_path_train, 'w').close()
if not os.path.exists(output_path_val):
    open(output_path_val, 'w').close()
if not os.path.exists(output_path_test):
    open(output_path_test, 'w').close()

sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))
sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))
from rule_based.rule_dataset import RuleDataset 


if not os.path.exists(rel_mapping_path) or include_inverse_flags==True or split_train_val_test_flag==True:

    ruledataset = RuleDataset(name=dataset_name2, large_data_hardcode_flag=False)
    # output_string_name = 'incl_inverse_' + output_striny_name

if split_train_val_test_flag:
    max_train_ts = ruledataset.train_data[:,3].max()
    max_val_ts = ruledataset.val_data[:,3].max()
    max_test_ts = ruledataset.test_data[:,3].max()
    print(f"Max train ts: {max_train_ts}, Max val ts: {max_val_ts}, Max test ts: {max_test_ts}")

    timestamp_orig2id = {ruledataset.timestamp_id2orig[i]: i for i in ruledataset.timestamp_id2orig.keys()}

max_line = 1e100 #90730 #2278405 # 610153
min_line = 0

# Load mappings
with open(rel_mapping_path, 'r', encoding='utf-8') as f:
    rel_mapping = {row[0]: row[rel_index] for i, row in enumerate(csv.reader(f, delimiter=';')) if i > 0}

with open(node_mapping_path, 'r', encoding='utf-8') as f:
    node_mapping = {row[node_mapping_index]: row[node_index]  for i, row in enumerate(csv.reader(f, delimiter=';')) if i > 0}

# Process edgelist and write output
with open(edgelist_path, 'r') as edgelist_file, \
    open(output_path, 'w') as output_file, \
    open(output_path_train, 'w') as output_file_train, \
    open(output_path_val, 'w') as output_file_val, \
    open(output_path_test, 'w') as output_file_test:
    # write to output_file_train, output_file_val, and output_file_test in parallel
    
    reader = csv.reader(edgelist_file)
    for i, row in enumerate(reader):
        first_open = True
        if i >= max_line:
            break
        if i > min_line:
            # timestep, head, tail, rel = map(int, row) # ts,head,tail,relation_type
            timestep, head, tail, rel = row
            timestep = int(timestep)
            timestep_mapped = timestamp_orig2id[timestep] if split_train_val_test_flag else timestep
            if split_train_val_test_flag:
                if timestep_mapped <= max_train_ts:
                    output_file = output_file_train

                elif timestep_mapped <= max_val_ts:
                    output_file = output_file_val
                else:
                    output_file = output_file_test
            head_str = node_mapping.get(head, f'unknown_{head}')
            rel_str = rel_mapping.get(rel, f'unknown_{rel}')
            tail_str = node_mapping.get(tail, f'unknown_{tail}')
            if split_train_val_test_flag:
                timestep = timestep_mapped
            else:
                timestep = timestep // ts_size  # Adjust timestep based on ts_size
            output_file.write(f"{head_str}\t{rel_str}\t{tail_str}\t{timestep}\n")

            if include_inverse_flags:
                if type_of_output == 'tgbid':
                    inverse_rel = ruledataset.get_inv_rel_id(rel)
                    
                elif type_of_output == 'string':
                    inverse_rel = 'inv_' + rel_str
                output_file.write(f"{tail_str}\t{inverse_rel}\t{head_str}\t{timestep}\n")

print(f"Processed {i - min_line} lines from the edgelist and saved to {output_path}")