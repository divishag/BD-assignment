# Team 42 Distributed Food Ordering — GitHub starter

This starter preserves the available VM4 Python files verbatim and provides fresh-cluster broker scripts. **It is not a completed runnable application repo** until original VM1 files and VM4 dependencies are checked and copied.

## Current roles (unchanged)
VM1 producer1 / consumer2 / Spark job2; VM2 Broker 1 + sole KRaft controller; VM3 (Dhanya) Broker 2; VM4 (Harsimran) consumer1 / Spark job1 / producer2.

VM2 ZeroTier `172.25.244.150`; VM3 ZeroTier `172.25.27.214`. These are explicit lab IP assumptions; update scripts if they change. Old VM3 `172.25.102.74` is NOT used. VM4's existing Python scripts contain VM2 as a bootstrap address and expect `~/food-ordering` paths: leave them unchanged and arrange those paths when deploying.

## IMPORTANT: fresh cluster vs existing cluster
These scripts are for a **NEW EMPTY CLUSTER**, not replacing the existing Broker 2 in place. Keep old VM2/VM3 Kafka data untouched. A fresh cluster on VM2 requires stopping its old Kafka broker/controller and using a distinct storage directory. Do not run two Kafka instances on the same ports. Do not create a fresh cluster unless the team/TA permits abandoning old Kafka records and offsets. For an existing-cluster broker replacement, use a separate planned recovery procedure, NOT `initialize_storage.sh`.

## Prerequisites
Kafka 3.9.2 installed at `/opt/kafka` on VM2 and VM3; compatible Java; ZeroTier network reachable, low packet loss; all processes stopped on ports 9092/9093 before fresh setup. `kafka-storage.sh` must exist. This package does not install Kafka or ZeroTier.

## Setup — only after agreeing to a fresh cluster
1. Clone repo on VM2 and VM3. `cd team42-github-starter` (adjust folder name if renamed).
2. On VM2: `bash vm2/setup.sh`; on VM3: `bash vm3/setup.sh`. If an old generated config exists, the script refuses to overwrite it; inspect/backup first. Review `scripts/check_config.sh`.
3. On VM2 ONLY, generate a NEW cluster ID: `/opt/kafka/bin/kafka-storage.sh random-uuid`. Share that exact ID privately with VM3.
4. On BOTH VMs, after verifying the new `~/team42-kafka-data` directory is EMPTY: `export CLUSTER_ID='PASTE_THE_SAME_NEW_ID'` then `bash scripts/initialize_storage.sh`. This operation is only for the new directory and refuses nonempty storage.
5. Start VM2 first: `bash vm2/start.sh` (foreground). In a second terminal, check `sudo ss -lntp | grep -E ':9092|:9093'` and `/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller 172.25.244.150:9093 describe --status`.
6. Start VM3: `bash vm3/start.sh` (foreground). Verify port 9092 on VM3.
7. On VM2: `bash scripts/create_topics.sh` then `bash scripts/verify_cluster.sh`. Wait until BOTH topics have 2 partitions, replication factor 2 and ISR includes 1,2 on both partitions.
8. Only then coordinate consumer start and fresh producer runs. Old consumer offsets do not carry to a new cluster.

## Stop and restart
`bash vm3/stop.sh` or `bash vm2/stop.sh` on the appropriate VM. Never stop VM2 in the broker-only fault-tolerance test: it is the sole controller. Start VM2 before VM3 when bringing up a fully stopped cluster. If a start hangs, inspect `/opt/kafka/logs/server.log` and connectivity rather than launching another process. Never remove Kafka `.lock` or delete existing data to force a start.

## GitHub publishing
Before pushing, copy original VM1 scripts into `vm1/`; verify VM4 scripts are the exact approved versions; add missing requirements/setup instructions; inspect data for personal information and secrets. Then `git init && git add . && git commit -m 'Add Team 42 pipeline and broker setup' && gh repo create team42-distributed-food-ordering --private --source=. --remote=origin --push` (requires `gh` authenticated). Invite teammates in GitHub settings. **No repository has been created or pushed by this package.**
