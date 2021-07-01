SRC_DIR=$1

mkdir -p $SRC_DIR/image

draw() {
  FILE=$1
  ylab=$2
  xlab=$3
  xs=$4
  xe=$5
  ys=$6
  ye=$7
  folder=$8
  prefix=$9
  script=${10}
  ptn=${11}
  clr=${12}
  gnuplot -e "data='${SRC_DIR}/${folder}/${FILE}'; output='${SRC_DIR}/image/${prefix}${FILE}.pdf'; xlab='${xlab}'; ylab='${ylab}'; xs=${xs}; xe=${xe}; ys=${ys}; ye=${ye}; ptn=${ptn}; clr=${clr}" -c ${script}.plt;
}

# distributed
draw q1 "Query Latency (s)" "#Nodes" -0.5 4.75 1 20000 distributed distributed_ bar_log 3 \'\#bebebe\'
draw q2 "Query Latency (s)" "#Nodes" -0.5 4.75 10 2000 distributed distributed_ bar_log 3 \'\#bebebe\'
draw q3 "Query Latency (s)" "#Nodes" -0.5 4.75 10 2000 distributed distributed_ bar_log 3 \'\#bebebe\'
draw q5 "Query Latency (s)" "#Nodes" -0.5 4.75 10 10000 distributed distributed_ bar_log 3 \'\#bebebe\'

# single machine
draw multi-events "Query Latency (ms)" "Datasets" -0.75 4 10 30000 single ./ bar_log 7 \'\#218521\'
draw real-time "Query Latency (ms)" "Datasets" -0.75 4 10 5000 single ./ bar_log 7 \'\#218521\'
draw roll-up-latency "Query Latency (ms)" "Datasets" -0.75 4 10 3000 single ./ bar_log 4 \'\#000000\'
draw q5 "Query Latency (ms)" "Datasets" -0.75 4 10 15000 single single_ bar_log 7 \'\#218521\'

draw roll-up-memory "Memory (MB)" "Datasets" -0.75 4 100 10000 single ./ bar_log 4 \'\#000000\'
draw multi-event-memory "Memory (MB)" "Datasets" -0.75 4 100 5000 single ./ bar_log 7 \'\#218521\'

draw compression-ratio "Storage (GB)" "Datasets" -0.75 4 0 25 single ./ bar3 7 \'\#218521\'