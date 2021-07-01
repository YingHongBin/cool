#! /usr/bin/env python
# -*- coding: utf-8 -*-

#Author:Ying HongBin

import pandas as pd

data = pd.read_csv('data.csv', header=None)
data.columns = ['O_ORDERKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT', 'C_NAME', 'C_ADDRESS', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT', 'N_NAME', 'N_COMMENT', 'R_NAME', 'R_COMMENT', 'app', 'user']
columns = data.columns
for col in columns:
    a = data[col].to_list()
    a = set(a)
    with open('dim.csv', 'a+') as f:
        if col in ['O_TOTALPRICE', 'O_ORDERDATE', 'C_ACCTBAL']:
            m = max(a)
            n = min(a)
            f.write(col + ',' + str(n) + '|' + str(m) + '\n')
            continue
        for i in a:
            f.write(col + ',' + str(i) + '\n')
