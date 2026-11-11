# Production deployment (Kubernetes)

Reference manifests for the production topology sized in
[`docs/capacity-planning.md`](../docs/capacity-planning.md) (30+ TB/month raw ingest).
They assume these operators are installed in the cluster:

| Component | Operator | Manifest |
|---|---|---|
| Kafka (KRaft, 3 controllers + 6 brokers) | [Strimzi](https://strimzi.io) 0.40+ | `kafka.yaml`, `kafka-topics.yaml` |
| ClickHouse Keeper (3 nodes) | [Altinity clickhouse-operator](https://github.com/Altinity/clickhouse-operator) 0.23+ | `clickhouse-keeper.yaml` |
| ClickHouse (4 shards x 2 replicas, S3 tiering) | Altinity clickhouse-operator | `clickhouse.yaml` |
| Flink job (application mode, HA, savepoint upgrades) | [Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/) 1.8+ | `flink.yaml` |
| Load generator (for load tests only) | - | `load-generator.yaml` |

## Before applying

1. Replace every `CHANGE_ME` (S3 bucket, region, image registry, passwords).
2. Build and push the images:
   ```bash
   docker build -t ghcr.io/CHANGE_ME/adtech-flink-job:1.0.0 flink-job && docker push ghcr.io/CHANGE_ME/adtech-flink-job:1.0.0
   docker build -t ghcr.io/CHANGE_ME/adtech-generator:1.0.0 generator && docker push ghcr.io/CHANGE_ME/adtech-generator:1.0.0
   ```
3. S3 access for ClickHouse and Flink is expected through workload identity (IRSA on EKS,
   Workload Identity on GKE) on the `clickhouse` and `flink` service accounts.
4. Check the Keeper service name the operator created (`kubectl -n adtech get svc`) and adjust
   `zookeeper.nodes` in `clickhouse.yaml` if your operator version names it differently.

## Apply

```bash
kubectl apply -k k8s/                       # namespace, secrets, Kafka, Keeper, ClickHouse, Flink
kubectl -n adtech get chi,chk,kafka,flinkdeployment
```

Then apply the schema once (all DDL is `ON CLUSTER`):

```bash
kubectl -n adtech run ch-schema --rm -it --image=clickhouse/clickhouse-server:24.3 \
  --env CH_HOST=chi-adtech-adtech-0-0 --env CH_PASSWORD=... -- bash   # then run scripts/apply-schema.sh
```

Note: in `infra/clickhouse/schema/07_dead_letters.sql`, change `kafka_broker_list` to
`adtech-kafka-kafka-bootstrap:9092` for this environment.
