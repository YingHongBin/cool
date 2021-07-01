if [ "$#" -ne 2 ]; then
  echo "run-single.sh <datasource> <query_id>"
  exit
fi

data=$1
qid=$2

if ! [ -d cube/${data}-tpch ]; then
  echo -e "cube/${data} not found"
  exit
fi

if [ $qid == "2" ]; then
  sed "s/indata/${data}-tpch/g" q2.json.template > fake-data-query.json
elif [ $qid == "3" ]; then
  sed "s/indata/${data}-tpch/g" q3.json.template > fake-data-query.json
fi

java -cp target/Cohana_v0.0.1-0.0.1-SNAPSHOT.jar com.nus.cohana.executor.LocalIcebergLoader cube ${data}-tpch

