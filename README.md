# t2

## Install

On (linux/OSX) JDK 1.8.0

Untar in ~/app and rename to spark and hadoop

[spark](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.4/spark-2.4.4-bin-without-hadoop-scala-2.12.tgz)

[hadoop](https://archive.apache.org/dist/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz)

## Building
```
$ bin/t2 build jar
```

## Running 


#### Settings

```bash
export ALLOWED_HOST_NAME=localhost
export APP_SECRET=superSecretkey
export KGX_VERSION=v0.1
export EXECUTOR_CORES=1
export DRIVER_CORES=1
export EXECUTOR_MEM=4g
export DRIVER_MEM=2g
export SPARK_SCRATCH_DIR=/tmp
export SPARK_DEPLOY_MODE=client
export SPARK_DRIVER_HOST=locahost
export SPARK_KUBERNETES_NAMESPACE=default
export SPARK_KUBERNETES_CONTAINER_IMAGE=renciorg/t2:spark-2.4.4-2.12-k8s-app
```

#### Starting services

```
bin/t2 spark master start
bin/t2 spark worker start
bin/t2 runWebServerLocal
```

