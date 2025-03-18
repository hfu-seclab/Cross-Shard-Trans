==========================================================================
---------------------------- MODULE our_process ----------------------------

EXTENDS Naturals, Sequences

CONSTANTS Shards, Users, A1, A2, B1, ShardA, ShardB

ASSUME Shards = {ShardA, ShardB}
ASSUME Users = {A1, A2, B1}

VARIABLES
    balances, ShardA_RTX_Status, ShardB_DTX_Status, Rollback_Status

(* 初始状态 *)
Init == 
    /\ balances = [u \in Users |-> IF u = A1 THEN 1000 ELSE 0] (* A1 余额初始为 1000，即 100 bit *)
    /\ ShardA_RTX_Status = "Pending"
    /\ ShardB_DTX_Status = "Pending"
    /\ Rollback_Status = "None"

(* 发送RTX交易 *)
SendRTX ==
    /\ ShardA_RTX_Status = "Pending"
    /\ balances[A1] >= 102   (* RTX交易金额为 102，即 10.2 bit *)
    /\ ShardA_RTX_Status' = "Committed"
    /\ balances' = [balances EXCEPT ![A1] = @ - 102, ![A2] = @ + 102]

(* RTX交易打包上链 *)
CommitRTX ==
    /\ ShardA_RTX_Status = "Committed"
    /\ ShardB_DTX_Status' = "Pending"

(* 发送DTX交易 *)
SendDTX ==
    /\ ShardB_DTX_Status = "Pending"
    /\ ShardA_RTX_Status = "Committed"
    /\ ShardB_DTX_Status' = "Committed"
    /\ balances' = [balances EXCEPT ![A2] = @ - 100, ![B1] = @ + 100] (* DTX交易金额为 100，即 10 bit *)

(* DTX交易打包上链 *)
CommitDTX ==
    /\ ShardB_DTX_Status = "Committed"
    /\ ShardA_RTX_Status = "Committed"

(* 回滚状态 *)
Rollback ==
    \/ /\ ShardA_RTX_Status = "Committed" /\ ShardB_DTX_Status /= "Committed"
       /\ Rollback_Status' = "Rollback"
       /\ balances' = [balances EXCEPT ![A1] = @ + 102, ![A2] = @ - 102]
       /\ ShardA_RTX_Status' = "Pending"
       /\ ShardB_DTX_Status' = "Pending"
    \/ /\ ShardA_RTX_Status /= "Committed" /\ ShardB_DTX_Status = "Committed"
       /\ Rollback_Status' = "Rollback"
       /\ balances' = [balances EXCEPT ![A2] = @ + 100, ![B1] = @ - 100]
       /\ ShardA_RTX_Status' = "Pending"
       /\ ShardB_DTX_Status' = "Pending"

(* 下一步 *)
Next == 
    \/ SendRTX
    \/ CommitRTX
    \/ SendDTX
    \/ CommitDTX
    \/ Rollback

(* 一致性 *)
Consistency == 
    /\ (ShardA_RTX_Status = "Committed" <=> ShardB_DTX_Status = "Committed")
    /\ (ShardA_RTX_Status = "Pending" <=> ShardB_DTX_Status = "Pending")

(* 原子性 *)
Atomicity ==
    /\ (ShardA_RTX_Status = "Committed" \/ ShardA_RTX_Status = "Pending")
    /\ (ShardB_DTX_Status = "Committed" \/ ShardB_DTX_Status = "Pending")

(* 规范 *)
SPECIFICATION == Init /\ [][Next]_<<balances, ShardA_RTX_Status, ShardB_DTX_Status, Rollback_Status>>

(* 属性检查 *)
ConsistencyCheck == Consistency
AtomicityCheck == Atomicity

=============================================================================
