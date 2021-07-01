. env.sh

for i in {0..15}
do
  let ip=$i+60
  ssh 10.0.0.${ip} "cd $COHANA_HOME/med-data; python convert.py data$i.csv" &
done
