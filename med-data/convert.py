import sys

filename = sys.argv[1]
idx = filename.find(".")
namenoext = filename[0:idx]

field = ["PID", "INSTITUTION", "O", "F", "B",
         "CASETYPE", "O_COMMENT", "F_COMMENT", "B_COMMENT",
         "DATE", "EVENT", "METRIC", "DID", "APP"]

outfile = open(namenoext + ".json", "w")
outfile.write("[\n")
lineno = 0
with open(filename) as fp:
  for line in fp:
    if lineno > 0:
      outfile.write(",\n")
    outfile.write("\t{ ")
    st = 0
    it = 0
    for i in range(0,len(field)):
      ed = line.find(",", st)
      if i < len(field) - 1 and ed == -1:
        print "tokenize error"
      if i > 0:
        outfile.write(", ")
      outfile.write("\"" + field[it] + "\":\"" + line[st:ed] + "\"")
      it = it + 1
      st = ed + 1
    outfile.write(" }")
    lineno = lineno + 1

outfile.write("\n]")
outfile.close()
print "{} lines converted.".format(lineno)
