# Cross-Shard-Trans

#### Step 1. extra data
```bash
python3 extra_data.py ETH_2000000_2999999_TXs.csv {shard_size}
```

#### Step 2. run blockchain
- first you need change the nodes ip addr in main.go
- the addr size equals the shard size

```bash
go run . -shard=0 -rate={rate_per_node} -node={node_count} -crp={scheme}
```
#### Step 3. parser outputs
```bash
python3 parser.py {shard_size}
```
