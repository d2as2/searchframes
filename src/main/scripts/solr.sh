export JAVA_HOME=/opt/bda/java/jdk1.8.0_102/
export PATH=$JAVA_HOME/bin:$PATH
java -cp solrtest-1.0-SNAPSHOT.jar:solrtest-1.0-SNAPSHOT-job.jar:/etc/hadoop/conf/   com.verizon.bda.trapezium.dal.solr.SolrTests