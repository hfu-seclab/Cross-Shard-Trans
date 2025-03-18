import json
import os


def load_json_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


def load_ledger(ledger_file):
    return load_json_file(ledger_file)


def parser_ledger(txs):
    transactions = []
    for tx in txs:
        if tx['amount'] != "0.0":
            transactions.append(tx)
    return transactions


# 假设文件名格式为 "transactionsX.json"
file_names = [
    "transactions2.json", "transactions3.json", "transactions4.json",
    "transactions5.json", "transactions6.json"
]

for file_name in file_names:
    if os.path.exists(file_name):
        ledger = load_ledger(file_name)
        transactions = parser_ledger(ledger)

        cross_shard_transactions = sum(1 for tx in transactions
                                       if tx["sendshard"] != tx["receivshard"])

        total_transactions = len(transactions)

        percentage = (cross_shard_transactions / total_transactions
                      ) * 100 if total_transactions > 0 else 0

        print(f"处理文件: {file_name}")
        print(f"总交易数: {total_transactions}")
        print(f"sendshard 不等于 receivshard 的交易数: {cross_shard_transactions}")
        print(f"占比: {percentage:.2f}%\n")
    else:
        print(f"文件 {file_name} 不存在。\n")
