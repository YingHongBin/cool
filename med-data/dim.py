#! /usr/bin/env python
# -*- coding: utf-8 -*-

#Author:Ying HongBin

import pandas as pd

if __name__ == '__main__':
    data = pd.read_csv('data.csv', header=None)
    data.columns = ['Country', 'Region', 'City', 'PID', 'INSTITUTION', 'O', 'F', 'B', 'CASETYPE', 'O_COMMENT', 'F_COMMENT', 'B_COMMENT', 'DATE', 'EVENT', 'METRIC', 'DID', 'APP']
    columns = data.columns
    with open('dim.csv', 'w+') as f:
        for col in columns:
            if col == 'DATE':
                a = data[col].tolist()
                f.write(col + ',' + min(a) + '|' + max(a) + '\n')
                continue
            if col == 'METRIC':
                f.write(col + ',' + str(data[col].min()) + '|' + str(data[col].max()) + '\n')
                continue
            for i in data[col].unique():
                f.write(col + ',' + str(i) + '\n')
