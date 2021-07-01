. env.sh

if [ "$#" -ne 1 ]; then
    echo -e "run-exp.sh <query_id>\nExample: run-exp.sh 1"
    exit
fi

NNODE=(1 2 4 8 16)

for nnode in ${NNODE[@]}
do
  $HADOOP_HOME/bin/hdfs dfs -rm /tmp/$1/results/*
  if [ $1 == "5a" ]; then
    $HADOOP_HOME/bin/hdfs dfs -rm -r /cube/q5b/version1
    $HADOOP_HOME/bin/hdfs dfs -cp -d /cube/q5a/version1 /cube/q5b/version1
  fi
  
  let ctr=0
  while [ $ctr -lt ${#HOSTS[@]} ]
  do
    if [ $ctr -eq 0 ]; then
      echo -e "starting master node ${HOSTS[ctr]} ... "
      ssh ${HOSTS[ctr]} "cd $COHANA_HOME; java -jar $COHANA_HOME/target/Cohana_v0.0.1-0.0.1-SNAPSHOT.jar BROKER" &
      sleep 2
    elif [ $ctr -gt $nnode ]; then
      break
    else
      echo -e "starting worker node ${HOSTS[ctr]} ... "
      ssh ${HOSTS[ctr]} "cd $COHANA_HOME; java -jar $COHANA_HOME/target/Cohana_v0.0.1-0.0.1-SNAPSHOT.jar WORKER" &
      sleep 2
    fi
    let ctr=$ctr+1
  done
  
  echo -e "running query on $CLIENT ... "
  ssh $CLIENT "cd $COHANA_HOME; java -cp target/Cohana_v0.0.1-0.0.1-SNAPSHOT.jar com.nus.clientService.Main q$1"
  let M=30
  #let M=165
  sleep $M
  ./stop-all.sh
done

