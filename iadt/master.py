import numpy as np
import pandas as pd

from utils import *

from tkinter import filedialog

import src
import coll
import result

def load_params():
    param_cust_group = load_sheet('iadt/params.xlsx', sheet_name='CUST_GROUP').set_index('cust_group_code', drop=True).to_dict()
    param_product = load_sheet('iadt/params.xlsx', sheet_name='PRODUCT').set_index('product_code', drop=True).to_dict()
    param_coll_type = load_sheet('iadt/params.xlsx', sheet_name='COLL_TYPE').set_index('coll_type_code', drop=True).to_dict()
    return param_cust_group, param_product, param_coll_type

def load_source_file(source_file_info):
    data_on_sheet = load_sheet(source_file_info[0], sheet_name=source_file_info[1], header=int(source_file_info[2])-1)
    data_off_sheet = load_sheet(source_file_info[0], sheet_name=source_file_info[3], header=int(source_file_info[4])-1)
    return data_on_sheet, data_off_sheet

def load_collateral(collateral_file_info):
    collateral = load_sheet(collateral_file_info[0], sheet_name = collateral_file_info[1], header=int(collateral_file_info[2])-1)
    return collateral

def load_cic(cic_file_info):
    cic = load_sheet(cic_file_info[0], sheet_name = cic_file_info[1], header=int(cic_file_info[2])-1)
    return cic

def load_jica_rdf_vnsat(jica_rdf_vnsat_file_info):
    jica_rdf_vnsat = load_sheet(jica_rdf_vnsat_file_info[0], sheet_name = jica_rdf_vnsat_file_info[1], header=int(jica_rdf_vnsat_file_info[2])-1)
    return jica_rdf_vnsat

def notify(msg):
    n = len(msg)
    print('-'*n)
    print(msg.upper())
    print('-'*n)

def run(source_file_info, collateral_file_info, cic_file_info, branch_info, jica_rdf_vnsat_file_info):
    notify('load data')
    param_cust_group, param_product, param_coll_type = load_params()
    data_on_sheet, data_off_sheet = load_source_file(source_file_info)
    collateral = load_collateral(collateral_file_info)
    cic = load_cic(cic_file_info)
    jica_rdf_vnsat = load_jica_rdf_vnsat(jica_rdf_vnsat_file_info)

    notify('transform source file')
    notify('step 1: merge, off balance sheet & calculate real class')
    master = src.transform_1(
        data_on_sheet=data_on_sheet,
        data_off_sheet=data_off_sheet,
        cic=cic,
        param_cust_group=param_cust_group,
        param_product=param_product,
        branch_info=branch_info,
        jica_rdf_vnsat=jica_rdf_vnsat,
    )

    notify('transform collateral file')
    notify('step 2: basic collateral transformation')
    collateral = coll.transform_2(
        collateral=collateral,
    )

    notify('step 3: distribute collateral value')
    collateral, coll_val_dist = coll.transform_3(
        collateral=collateral,
        master=master,
        param_coll_type=param_coll_type,
    )

    notify('prepare result')
    notify('step 4: master by cif')
    master_by_cif = result.transform_4(
        master,
        coll_val_dist,
    )

    notify('export data')
    filename = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=(("Excel workbook", "*.xlsx"), ("All Files", "*.*")))
    with pd.ExcelWriter(filename, engine='openpyxl', mode='w') as writer:
        print('Master by ref ...')
        master.to_excel(writer, index=False, sheet_name='MASTER')
        print('Master by cif ...')
        master_by_cif.to_excel(writer, index=False, sheet_name='MASTER BY CIF')
        print('Collateral ...')
        collateral.to_excel(writer, index=False, sheet_name='COLLATERAL')
        print('Collateral value distribution ...')
        coll_val_dist.to_excel(writer, index=False, sheet_name='COLL_VAL_DIST')
    print('DONE')