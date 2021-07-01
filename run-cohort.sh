if [ "$#" -ne 1 ]; then
  echo "run-single.sh <datasource>"
  exit
fi

data=$1

if ! [ -d cube/${data}-a ]; then
  echo -e "cube/${data} not found"
  exit
fi

sed "s/indata/${data}-a/g" cohort-query.json.template > cohort-query.json

java -cp target/Cohana_v0.0.1-0.0.1-SNAPSHOT.jar com.nus.cohana.executor.LocalCohortLoader cube ${data}-a

