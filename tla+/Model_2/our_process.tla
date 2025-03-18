==========================================================================
---------------------------- MODULE our_process ----------------------------

EXTENDS Naturals, Sequences

CONSTANTS Shards, Users, A1, A2, B1, ShardA, ShardB

ASSUME Shards = {ShardA, ShardB}
ASSUME Users = {A1, A2, B1}

VARIABLES
    balances, ShardA_RTX_Status, ShardB_DTX_Status, Rollback_Status

(* ��ʼ״̬ *)
Init == 
    /\ balances = [u \in Users |-> IF u = A1 THEN 1000 ELSE 0] (* A1 ����ʼΪ 1000���� 100 bit *)
    /\ ShardA_RTX_Status = "Pending"
    /\ ShardB_DTX_Status = "Pending"
    /\ Rollback_Status = "None"

(* ����RTX���� *)
SendRTX ==
    /\ ShardA_RTX_Status = "Pending"
    /\ balances[A1] >= 102   (* RTX���׽��Ϊ 102���� 10.2 bit *)
    /\ ShardA_RTX_Status' = "Committed"
    /\ balances' = [balances EXCEPT ![A1] = @ - 102, ![A2] = @ + 102]

(* RTX���״������ *)
CommitRTX ==
    /\ ShardA_RTX_Status = "Committed"
    /\ ShardB_DTX_Status' = "Pending"

(* ����DTX���� *)
SendDTX ==
    /\ ShardB_DTX_Status = "Pending"
    /\ ShardA_RTX_Status = "Committed"
    /\ ShardB_DTX_Status' = "Committed"
    /\ balances' = [balances EXCEPT ![A2] = @ - 100, ![B1] = @ + 100] (* DTX���׽��Ϊ 100���� 10 bit *)

(* DTX���״������ *)
CommitDTX ==
    /\ ShardB_DTX_Status = "Committed"
    /\ ShardA_RTX_Status = "Committed"

(* �ع�״̬ *)
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

(* ��һ�� *)
Next == 
    \/ SendRTX
    \/ CommitRTX
    \/ SendDTX
    \/ CommitDTX
    \/ Rollback

(* һ���� *)
Consistency == 
    /\ (ShardA_RTX_Status = "Committed" <=> ShardB_DTX_Status = "Committed")
    /\ (ShardA_RTX_Status = "Pending" <=> ShardB_DTX_Status = "Pending")

(* ԭ���� *)
Atomicity ==
    /\ (ShardA_RTX_Status = "Committed" \/ ShardA_RTX_Status = "Pending")
    /\ (ShardB_DTX_Status = "Committed" \/ ShardB_DTX_Status = "Pending")

(* �淶 *)
SPECIFICATION == Init /\ [][Next]_<<balances, ShardA_RTX_Status, ShardB_DTX_Status, Rollback_Status>>

(* ���Լ�� *)
ConsistencyCheck == Consistency
AtomicityCheck == Atomicity

=============================================================================
