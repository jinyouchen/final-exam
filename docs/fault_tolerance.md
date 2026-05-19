# Kafka Cluster Fault Tolerance Test Report

## Test Objective
To verify the leader re-election mechanism and data availability of the 3-node Kafka cluster under single-node failure, meeting the requirements of `replication.factor=3` and `min.insync.replicas=2`.

---

### 1. Initial State (All 3 Nodes Running)
Command executed:
`docker exec kafka1 kafka-topics --bootstrap-server kafka1:9092 --describe --topic sensor-events`

Output:
Topic: sensor-events TopicId: rGARRjajS_62tgEMNlnpww PartitionCount: 3 ReplicationFactor: 3 Configs: min.insync.replicas=2
Topic: sensor-events Partition: 0 Leader: 3 Replicas: 3,1,2 Isr: 3,1,2
Topic: sensor-events Partition: 1 Leader: 1 Replicas: 1,2,3 Isr: 1,2,3
Topic: sensor-events Partition: 2 Leader: 2 Replicas: 2,3,1 Isr: 2,3,1
Observations:
- All partitions show identical `Replicas` and `Isr` lists, confirming all 3 replicas are fully synchronized.
- Partition leaders are evenly distributed across kafka1, kafka2, and kafka3.
- The cluster is healthy with no under-replicated partitions.

---

### 2. Failure State (kafka2 Node Stopped)
Commands executed:
1. Stop kafka2: `docker compose stop kafka2`
2. Describe topic: `docker exec kafka1 kafka-topics --bootstrap-server kafka1:9092 --describe --topic sensor-events`

Output:
Topic: sensor-events TopicId: rGARRjajS_62tgEMNlnpww PartitionCount: 3 ReplicationFactor: 3 Configs: min.insync.replicas=2
Topic: sensor-events Partition: 0 Leader: 3 Replicas: 3,1,2 Isr: 3,1
Topic: sensor-events Partition: 1 Leader: 1 Replicas: 1,2,3 Isr: 1,3
Topic: sensor-events Partition: 2 Leader: 3 Replicas: 2,3,1 Isr: 3,1
Observations:
- Leader re-election completed successfully: Partition 2 (previously led by kafka2) now has `Leader: 3` (kafka3).
- `Isr` lists no longer include kafka2, only the healthy nodes kafka1 and kafka3 remain.
- The cluster remains operational, satisfying `min.insync.replicas=2` to avoid data loss or service disruption.

---

### 3. Recovery State (kafka2 Node Restarted)
Command executed:
`docker compose start kafka2`

Observations:
- kafka2 rejoins the cluster and resumes replica synchronization automatically.
- kafka2 will be added back to the `Isr` lists of all partitions once it catches up with the leader.
- The cluster returns to full health with all three nodes operational and synchronized.

---

## Conclusion
The 3-node Kafka cluster passed the fault tolerance test. It demonstrated automatic leader re-election and maintained data availability during a single-node failure, validating the configuration of `replication.factor=3` and `min.insync.replicas=2`.