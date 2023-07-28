export K8S_API_URL="https://rancher.dtcsolution.vn/k8s/clusters/local"

/opt/spark/bin/spark-submit \
    --master k8s://$K8S_API_URL \
    --deploy-mode cluster \
    --conf spark.kubernetes.container.image=apache/spark:3.3.1 \
    --conf spark.executor.instances=2 \
    --conf spark.kubernetes.submission.waitAppCompletion=true \
    --conf spark.kubernetes.namespace=dev \
    --conf spark.kubernetes.authenticate.submission.oauthToken=kubeconfig-user-7f4ftqj24d:tjlrlxgk2lb2nc44mnxgkptjc845d7tnjd9tx67k9h9c2s7hklqmmz \
    --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0,org.projectnessie:nessie-spark-3.2-extensions:0.43.0,software.amazon.awssdk:bundle:2.17.178,software.amazon.awssdk:url-connection-client:2.17.178 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa \
    --class KafkafStream \
    local:///opt/spark/work-dir/kafkastream_iceberg.jar


