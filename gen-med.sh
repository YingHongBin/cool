. env.sh

if [ "$#" -ne 1 ]; then
  echo "gen-tpch.sh <data_source_name>"
  exit
fi

WORKER=(10.0.0.80 10.0.0.81 10.0.0.82 10.0.0.83 10.0.0.84 10.0.0.85 10.0.0.86 10.0.0.87
        10.0.0.88 10.0.0.89 10.0.0.90 10.0.0.91 10.0.0.92 10.0.0.93 10.0.0.94 10.0.0.95)
for i in {0..15}
do
  let j=$(($i%16))
  ssh ${WORKER[j]} "cd $COHANA_HOME; ./do-gen-med.sh $1 $i" &
done

