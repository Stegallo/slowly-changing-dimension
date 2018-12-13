FROM ubuntu

RUN apt-get update \
    && apt-get install -y \
    apt-utils \
    curl \
    wget \
    python3-minimal \
    python3-setuptools \
    python3-pip \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && ln -s /usr/bin/pip3 /usr/bin/pip

RUN apt-get install openjdk-8-jdk-headless -qq > /dev/null
RUN wget -q http://apache.osuosl.org/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
RUN tar xf spark-2.4.0-bin-hadoop2.7.tgz

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV SPARK_HOME=/spark-2.4.0-bin-hadoop2.7
ENV PATH $PATH:$SPARK_HOME/bin

RUN pip install py4j==0.10.4 \
    && pip install pytest==3.4.0 \
    && pip install boto3==1.5.22 \
    && pip install requests==2.18.4 \
    && pip install dict==0.0.42 \
    && pip install configparser==3.5.0 \
    && pip install pandas==0.22.0 \
    && pip install mock==2.0.0 \
    && pip install pytest-dependency==0.3.2 \
    && pip install pyyaml

RUN pip install pyspark==2.4.0

WORKDIR $SPARK_HOME
CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]

WORKDIR /
