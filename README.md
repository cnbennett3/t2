# t2

untar in ~/app and rename to spark and hadoop

[spark](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.4/spark-2.4.4-bin-without-hadoop-scala-2.12.tgz)

[hadoop](https://archive.apache.org/dist/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz)

in shell 1:
```
t2 spark master start
```
in shell 2
```
t2 spark worker start
```
in shell 3
```
t2 spark shell
```
then, at the scala prompt:
```
:load load.scala
```
