#
# Copyright (c) Renaissance Computing Institute.
# Distributed under the terms of the MIT License.
#
FROM openjdk:8-jdk-slim


LABEL maintainer="RENCI <info@renci.org>"

# Be root to administer the system core.
USER root
ENV INSTALL_DIR=/usr/local

# Least privilege: create a non root user.
ENV USER spark
ENV HOME /home/$USER
WORKDIR $HOME
ENV UID 1000
RUN adduser --disabled-login --home $HOME --shell /bin/bash --uid $UID $USER && \
	chown -R $UID:$UID $HOME

# Install wget git and sbt
# add key for sbt
RUN apt-get -y update
RUN apt-get install -y gnupg
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823
RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN apt-get -y update && apt-get install --no-install-recommends -y git sbt
RUN sbt version


# Install tini 
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini

RUN set -ex && \
# Install OS level libraries and tools.
	apt-get update && \
	ln -s /lib /lib64 && \
	apt-get install -y bash libc6 libpam-modules libnss3 wget python3 python3-pip && \
	chmod +x /usr/bin/tini && \
	rm /bin/sh && \
	ln -sv /bin/bash /bin/sh && \
	ln -sv /usr/bin/tini /sbin/tini && \
	echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
	chgrp root /etc/passwd && chmod ug+rw /etc/passwd && \
	ln -sv /usr/bin/python3 /usr/bin/python && \
	ln -sv /usr/bin/pip3 /usr/bin/pip # && \
#	rm -rf /var/cache/apt/*
	
# Spark dependencies
WORKDIR /opt
ENV SPARK_VERSION=2.4.4 \
	HADOOP_VERSION=2.6.5 \
	INSTALL_DIR=/usr/local

# setup spark and hadoop
RUN wget --no-verbose https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop-scala-2.12.tgz && \
    tar xzf spark-${SPARK_VERSION}-bin-without-hadoop-scala-2.12.tgz && \
    rm spark-${SPARK_VERSION}-bin-without-hadoop-scala-2.12.tgz && \
    wget --no-verbose https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    rm hadoop-${HADOOP_VERSION}.tar.gz && \
    # Link these to the install dir.
    ln -s $PWD/spark-${SPARK_VERSION}-bin-without-hadoop-scala-2.12 $INSTALL_DIR/spark && \
    ln -s $PWD/hadoop-${HADOOP_VERSION} $INSTALL_DIR/hadoop


RUN cp $INSTALL_DIR/spark/kubernetes/dockerfiles/spark/entrypoint.sh /opt/


# Set Spark related environment variables. Bind our custom hadoop version.
ENV JAVA_HOME=/usr/local/openjdk-8 \
	SPARK_HOME=$INSTALL_DIR/spark \
	SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info" \
	HADOOP_HOME=$INSTALL_DIR/hadoop \
	PATH=$PATH:$HADOOP_HOME/bin:$SPARK_HOME/bin \
	LD_LIBRARY_PATH=$HADOOP_HOME/lib/native \
	SPARK_DIST_CLASSPATH=/usr/local/hadoop/etc/hadoop:/usr/local/hadoop/share/hadoop/common/lib/*:/usr/local/hadoop/share/hadoop/common/*:/usr/local/hadoop/share/hadoop/hdfs:/usr/local/hadoop/share/hadoop/hdfs/lib/*:/usr/local/hadoop/share/hadoop/hdfs/*:/usr/local/hadoop/share/hadoop/mapreduce/lib/*:/usr/local/hadoop/share/hadoop/mapreduce/*:/usr/local/hadoop/share/hadoop/yarn:/usr/local/hadoop/share/hadoop/yarn/lib/*:/usr/local/hadoop/share/hadoop/yarn/*
ENV SPARK_CLASSPATH=$INSTALL_DIR/spark/jars/*:$SPARK_DIST_CLASSPATH

# Overlay required dependencies required by Hadoop and Minio.
WORKDIR $SPARK_HOME/jars
RUN rm kubernetes*
RUN wget --no-verbose https://stars.renci.org/var/lib/kubernetes-client-4.6.4.jar \
    && wget --no-verbose https://stars.renci.org/var/lib/kubernetes-model-4.6.4.jar \
    && wget --no-verbose https://stars.renci.org/var/lib/kubernetes-model-common-4.6.4.jar


WORKDIR $SPARK_HOME/work-dir
RUN chmod g+w $SPARK_HOME/work-dir


# Go home.
WORKDIR /home/$USER

# Install t2.
#RUN cd $HOME
#ENV GIT_BRANCH=jsonPerformance
#
#RUN git clone --verbose https://github.com/YaphetKG/t2.git --branch ${GIT_BRANCH} --single-branch $HOME/t2
#
#WORKDIR $HOME/t2
#RUN sbt
#RUN bin/t2 build jar
ENV T2_JAR=t2-scala-2.12.8.jar
RUN wget --no-verbose https://stars.renci.org/var/kgx_data/${T2_JAR}
RUN mv ${T2_JAR} ${INSTALL_DIR}/spark/jars


# Be a non root user.
USER $USER

ENTRYPOINT [ "/opt/entrypoint.sh" ]





