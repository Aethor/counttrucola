import csv
import os
import sys
# from rule_based.rule_dataset import RuleDataset 



dataset = 'wikidata'
dataset_name = 'tkgl_'+dataset
dataset_name2 = 'tkgl-'+dataset


print("Current sys.path:", sys.path)

# File paths
edgelist_path = os.path.join(sys.path[0], dataset_name, dataset_name2+'_edgelist.csv')
rel_mapping_path = os.path.join(sys.path[0],dataset_name, 'rel_mapping.csv')
node_mapping_path = os.path.join(sys.path[0],dataset_name, 'node_mapping.csv')


type_of_output = 'tgbid'  # or 'string' Type of output to generate

include_inverse_flags = False  # Set to True if you want to include inverse relations
split_train_val_test_flag = True

output_name = 'relation2id_int.txt'
output_name_node = 'entity2id_int.txt'
output_path = os.path.join(sys.path[0],dataset_name, output_name)
output_path_node = os.path.join(sys.path[0],dataset_name, output_name_node)


# Load mappings
with open(rel_mapping_path, 'r', encoding='utf-8') as f:
    rel_mapping = {row[0]: row[1] for i, row in enumerate(csv.reader(f, delimiter=';')) if i > 0}

with open(node_mapping_path, 'r', encoding='utf-8') as f:
    node_mapping = {row[0]: row[1]  for i, row in enumerate(csv.reader(f, delimiter=';')) if i > 0}


# ruledataset = RuleDataset(name=dataset_name2, large_data_hardcode_flag=False)

for rel in rel_mapping:
    with open(output_path, 'a', encoding='utf-8') as f:
        f.write(f"{rel}\t{rel_mapping[rel]}\n")

for node in node_mapping:
    with open(output_path_node, 'a', encoding='utf-8') as f:
        f.write(f"{node}\t{node_mapping[node]}\n")


print(f"Written relation mapping to {output_path}")