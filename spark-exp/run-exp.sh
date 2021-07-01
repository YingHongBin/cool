SPARK_HOME=/users/yuecong/spark-2.4.4-bin-hadoop2.7
NNODE=(16 8 4 2 1)

echo -e "\"#records\"\t\"Execution Time\"" > result
for nnode in ${NNODE[@]}
do
    echo -e -n "$nnode\t" >> result
    ssh slave-45 "cd $SPARK_HOME; ./sbin/stop-all.sh; cp query/slaves-${nnode} conf/slaves; ./sbin/start-all.sh; exit;"
    hdfs dfs -rm -f /sparks/$1/results/*
    spark-submit --master spark://10.0.0.45:7077 --class Query target/scala-2.12/query-project_2.12-1.0.jar $1 >> result
done
