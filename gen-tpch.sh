. env.sh

if [ "$#" -ne 1 ]; then
  echo "gen-tpch.sh <data_source_name>"
  exit
fi
mkdir -p cube/$1
mkdir -p cube/$1/version1

cd tpch
rm -f *.tbl

for i in {3..7}
do
  cd $DBGEN
  ./dbgen -f -s 10
  sed -i 's/,/./g' orders.tbl
  sed -i 's/,/./g' customer.tbl
  sed -i 's/,/./g' region.tbl
  sed -i 's/,/./g' nation.tbl
  mv *.tbl $COHANA_HOME/tpch
  
  cd $COHANA_HOME/tpch
  python merge.py
  python dim.py
  mv data.csv data${i}.csv
  mv dim.csv dim${i}.csv

  java -Xmx16384m -cp ../target/Cohana_v0.0.1-0.0.1-SNAPSHOT.jar com.nus.cohana.loader.LocalLoader ./table.yaml ./dim${i}.csv ./data${i}.csv ../cube/$1/version1 1000000
done
