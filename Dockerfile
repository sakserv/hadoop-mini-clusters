FROM centos:latest

RUN yum install maven wget unzip -y
RUN cd /tmp && wget https://github.com/sakserv/hadoop-mini-clusters/archive/master.zip && unzip master.zip && cd hadoop-mini-clusters-master && mvn clean test
