. env.sh

for ip in "${HOSTS[@]}"
do
  ssh $ip "kill -9 \$(lsof -t -i:9001);exit"
done
