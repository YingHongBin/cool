set terminal pdfcairo color font "Helvetica, 22" linewidth 2 size 4,4
set output output
set boxwidth 1.0
set style fill transparent pattern border lt -0.5
set style data histograms
set style histogram clustered gap 2 title 
set xtics nomirror 
set ytics nomirror
set xtics font ",18"
set xlabel xlab offset 0,0.5
set ylabel ylab 

set terminal pdfcairo size 4,3.5
set key width -1 font ",20" samplen 2.2
#set grid ytics
set xrange [xs : xe] noreverse nowriteback
set yrange [ys : ye] noreverse nowriteback
set key horizontal outside top left reverse Left width 1.5 height 0
plot data using 2:xtic(1) ti col, \
'' using 3 ti col fillstyle pattern 4 lc rgb '#000000', \
'' using 4 ti col fillstyle pattern 3 lc rgb '#bebebe'