import numpy as np
import pandas as pd

def load_sheet(file, sheet_name=0, header=0):
    print('Reading sheet {} from {} ...'.format(sheet_name, file))
    data = pd.read_excel(file, sheet_name=sheet_name, header=header)
    print('{} row(s) read'.format(data.shape[0]))
    return data