import json
import os
import random
from datetime import datetime
import sys

# 获取当前目录路径
current_directory = os.getcwd()

all_files = []


def load_json_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


def load_ledger(ledger_file):
    return load_json_file(ledger_file)


def parser_ledger(ledger):
    transactions = []
    for block in ledger['Blocks']:
        if len(block['Transactions']) > 0:
            blockTime = block['BlockID']
            for transaction in block['Transactions']:
                transaction['block_time'] = blockTime
                transactions.append(transaction)
    return transactions


def is_id_in_list(target_id, list_of_dicts):
    return any(d['id'] == target_id for d in list_of_dicts)


def process_ledger_file(files, directory_path):
    for ledger_file in files:
        # print("Loading Shard %d Data ....", ledger_file.split('_')[2])
        other_files = [f for f in files if f != ledger_file]
        cur_ledger = load_ledger(directory_path + ledger_file)
        cur_txs = parser_ledger(cur_ledger)
        other_txs = []
        for _ledger_file in other_files:
            _ledger = load_ledger(directory_path + _ledger_file)
            txs = parser_ledger(_ledger)
            for tx in txs:
                other_txs.append(tx)
        intra_txs = []
        cross_txs = []
        for tx in cur_txs:
            shard_mark = ledger_file.split('_')[2]
            if tx['receivshard'] == shard_mark and tx[
                    'sendshard'] != shard_mark:
                cross_txs.append(tx)
            if tx['receivshard'] == shard_mark and tx[
                    'sendshard'] == shard_mark:
                intra_txs.append(tx)
        # print("分片内事务共 %d 条", len(intra_txs))
        # print("跨分片事务共 %d 条", len(cross_txs))

        intra_diffs = []
        cross_diffs = []

        for itx in intra_txs:
            submit_time_str = itx['submit_time']
            block_time_str = itx['block_time']
            submit_time_value = int(submit_time_str)
            block_time_value = int(block_time_str.split('-')[1])
            submit_time_dt = datetime.fromtimestamp(submit_time_value / 1000.0)
            block_time_dt = datetime.fromtimestamp(block_time_value / 1000.0)
            time_difference = block_time_dt - submit_time_dt
            time_difference_seconds = time_difference.total_seconds()
            intra_diffs.append(time_difference_seconds)

        for stx in cross_txs:
            submit_time_str = stx['submit_time']
            block_time_str = stx['block_time']
            submit_time_value = int(submit_time_str)
            block_time_value = int(block_time_str.split('-')[1])
            submit_time_dt = datetime.fromtimestamp(submit_time_value / 1000.0)
            block_time_dt = datetime.fromtimestamp(block_time_value / 1000.0)
            time_difference = block_time_dt - submit_time_dt
            time_difference_seconds = time_difference.total_seconds()
            cross_diffs.append(time_difference_seconds)

        print("CrossProtocal: " + ledger_file.split('_')[0])
        if cross_diffs:
            max_cross_diff = max(cross_diffs)
            min_cross_diff = min(cross_diffs)
            print(
                f"Maximum Cross TXs Time Difference: {max_cross_diff:.3f} seconds"
            )
            print(
                f"Minimum Cross TXs Time Difference: {min_cross_diff:.3f} seconds"
            )
        else:
            print("No cross transactions found.")

        # 计算平均时间差
        avg_time_diff_list1 = sum(intra_diffs) / len(
            intra_diffs) if intra_diffs else 0
        avg_time_diff_list2 = sum(cross_diffs) / len(
            cross_diffs) if cross_diffs else 0

        # 随机选择并打印 list1 和 list2 的 10 个数据
        # sample_list1 = random.sample(intra_diffs, min(10, len(intra_diffs)))
        # sample_list2 = random.sample(cross_diffs, min(10, len(cross_diffs)))

        # print("CrossProtocal: " + ledger_file.split('_')[0])
        # print(
        #     f"Intra TXs Average Time Difference : {avg_time_diff_list1:.3f} seconds"
        # )
        # print(f"Sample from list1: {sample_list1}")
        # print(
        #     f"Cross TXs Average Time Difference : {avg_time_diff_list2:.3f} seconds"
        # )
        # print(f"Sample from list2: {sample_list2}")


def parse_log_file(file_path):
    parsed_data = []

    with open(file_path, 'r') as file:
        for line in file:
            # 去掉行末的换行符
            line = line.strip()
            # 使用下划线分割行数据
            parts = line.split('_')
            if len(parts) == 3:
                # 构造字典
                entry = {
                    'id': parts[0],
                    'status': parts[1],
                    'retrycount': int(parts[2])  # 将retrycount转换为整数
                }
                parsed_data.append(entry)

    return parsed_data


def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <shardSize>")
        sys.exit(1)
    shardSize = sys.argv[1]
    directory_path = f"./outputs/{shardSize}/"
    if not os.path.exists(directory_path):
        print(f"Directory {directory_path} does not exist.")
        sys.exit(1)

    ledger_files = {}
    transactions_files = []

    # 遍历目录中的文件
    for filename in os.listdir(directory_path):
        if filename.endswith(".json"):
            if "ledger" in filename:
                # 提取前缀
                prefix = filename.split('_')[0]

                # 初始化前缀列表如果不存在
                if prefix not in ledger_files:
                    ledger_files[prefix] = []

                # 添加文件到对应的前缀列表
                ledger_files[prefix].append(filename)

        elif filename.endswith(".log"):
            # 如果需要处理 .log 文件，可以在这里添加逻辑
            pass

    # 输出结果
    print("Ledger Files:")
    for scheme, files in ledger_files.items():
        # print(f"{scheme}: {files}")
        process_ledger_file(files, directory_path)


if __name__ == "__main__":
    main()
