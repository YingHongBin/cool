if [ "$#" -ne 1 ]; then
  echo "run-single.sh <datasource>"
  exit
fi

data=$1

if ! [ -d cube/${data}-a ]; then
  echo -e "cube/${data} not found"
  exit
fi

rm -rf cube/${data}-b
cp -r cube/${data}-a cube/${data}-b
sed "s/indata/${data}-a/g" q5a.json.template > temporary.json
sed "s/outdata/${data}-b/g" temporary.json > cohort-query.json
sed "s/indata/${data}-b/g" q5b.json.template > fake-data-query.json
rm temporary.json

java -cp target/Cohana_v0.0.1-0.0.1-SNAPSHOT.jar com.nus.cohana.executor.LocalCohortLoader cube ${data}-a
java -cp target/Cohana_v0.0.1-0.0.1-SNAPSHOT.jar com.nus.cohana.executor.LocalIcebergLoader cube ${data}-b

