. env.sh

mkdir -p cube/$1
mkdir -p cube/$1/version1
cp -f med-data/table.yaml cube/$1/version1/
cp -f sogamo/cube.yaml cube/$1/version1/
cd med-data

if ! [ -f data$2.csv ]; then
  let st=15300*$2
  let ed=$st+15300
  python make.py $st $ed
  python dim.py
  mv data.csv data$2.csv
  mv dim.csv dim$2.csv
fi
echo -e "done generating csv $2"

java -Xmx16384m -cp ../target/Cohana_v0.0.1-0.0.1-SNAPSHOT.jar com.nus.cohana.loader.LocalLoader ./table.yaml ./dim$2.csv ./data$2.csv ../cube/$1/version1 1000000
echo -e "done compressing data $2"
