import pandas as pd
import hashlib
import argparse
import json
import random


def wei_to_eth(wei):
    eth = int(wei) / 10**18
    return eth


def get_shard(address, num_shards=3):
    hash_value = int(hashlib.sha256(address.encode('utf-8')).hexdigest(), 16)
    shard = hash_value % num_shards
    return shard


def generate_unique_id(existing_ids):
    while True:
        random_id = f"{random.randint(0, 999999999999):012}"
        if random_id not in existing_ids:
            existing_ids.add(random_id)
            return random_id


def read_csv_with_pandas(file_path, num_shards=3, output_file=""):
    output_file = "transactions" + str(num_shards) + ".json"
    df = pd.read_csv(file_path)
    if {'from', 'to', 'value'}.issubset(df.columns):
        # Filter out transactions where 'from' == 'to'
        df = df[(df['from'] != df['to'])]

        transactions = df[['from', 'to', 'value']].copy()

        # Generate unique IDs for each transaction
        existing_ids = set()
        transactions['id'] = transactions.apply(
            lambda _: generate_unique_id(existing_ids), axis=1)

        transactions['fromid'] = transactions['from'].astype(str).str.replace(
            '^0x', '', regex=True)
        transactions['toid'] = transactions['to'].astype(str).str.replace(
            '^0x', '', regex=True)
        transactions['amount'] = transactions['value'].apply(wei_to_eth)
        transactions['sendshard'] = transactions['from'].apply(
            lambda x: get_shard(str(x), num_shards))
        transactions['receivshard'] = transactions['to'].apply(
            lambda x: get_shard(str(x), num_shards))
        transactions['status'] = "PrePrepare"
        # Convert to list of dictionaries
        transaction_dicts = transactions.to_dict(orient='records')

        transaction_dicts = [{
            key: str(value)
            for key, value in transaction.items()
        } for transaction in transaction_dicts]

        # Save as JSON file
        with open(output_file, 'w') as f:
            json.dump(transaction_dicts, f, indent=4)
    else:
        print("CSV file is missing required columns: 'from', 'to', 'value'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some transactions.')
    parser.add_argument('file_path', type=str, help='Path to the CSV file')
    parser.add_argument('num_shards',
                        type=int,
                        nargs='?',
                        default=3,
                        help='Number of shards (default: 3)')
    args = parser.parse_args()
    read_csv_with_pandas(args.file_path, args.num_shards)
