# t2

untar in ~/app and rename to spark and hadoop

[spark](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.4/spark-2.4.4-bin-without-hadoop-scala-2.12.tgz)

[hadoop](https://archive.apache.org/dist/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz)

create a python3.7+ virtual environment

clone [kgx](https://github.com/NCATS-Tangerine/kgx) in the repo, overwriting exixting kgx

then restore kgx/kgx/transformer.py with `git checkout kgx/kgx/transformer.py`

import some robokop data via kgx
```
t2 robokop import
```
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
