# t2

## Install

On osx, running JDK 1.8.0

Untar in ~/app and rename to spark and hadoop

[spark](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.4/spark-2.4.4-bin-without-hadoop-scala-2.12.tgz)

[hadoop](https://archive.apache.org/dist/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz)

Create a python3.7+ virtual environment

Clone [kgx](https://github.com/NCATS-Tangerine/kgx) inside this repo, overwriting exixting kgx directory.

Then restore kgx/kgx/transformer.py with `git checkout kgx/kgx/transformer.py`

## Running

Import some robokop data via kgx
```
t2 robokop import
```
In a shell:
```
t2 spark master start
```
In another shell
```
t2 spark worker start
```
In another shell
```
t2 spark shell
```
Then, at the scala prompt:
```
:load load.scala
```

