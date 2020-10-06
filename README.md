# t2

## Install

On (linux/OSX) JDK 1.8.0

Untar in ~/app and rename to spark and hadoop

[spark](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.4/spark-2.4.4-bin-without-hadoop-scala-2.12.tgz)

[hadoop](https://archive.apache.org/dist/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz)

## Building
```
<repo-root>$ bin/t2 buildjar
```

## Running 

In a shell:
```
<repo-root>$ bin/t2 spark master start
```
In another shell
```
<repo-root>$ bin/t2 spark worker start
```
In another shell
```
<repo-root>$ t2 spark shell
```

To start loading a graph, an example script could be used. 

```
:load bin/examples/makeGraph.sc
```

