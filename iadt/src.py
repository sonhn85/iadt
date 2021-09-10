import numpy as np
import pandas as pd

def transform_1(data_on_sheet, data_off_sheet, cic, param_cust_group, param_product, branch_info, jica_rdf_vnsat):

    data_on_sheet.columns = data_on_sheet.columns.str.lower()

    print('Filtering on balance sheet data ...')
    parent_branchs = branch_info[0].split(',')
    excluded_branchs = branch_info[1].split(',')
    data_on_sheet = data_on_sheet[data_on_sheet['branch_parent_code'].isin(parent_branchs)]
    data_on_sheet = data_on_sheet[~data_on_sheet['branch_code'].isin(excluded_branchs)]
    data_on_sheet = data_on_sheet[~data_on_sheet['gl'].isin([9711])]
    print('{} row(s) remain'.format(data_on_sheet.shape[0]))

    cols = [
        'data_date',
        'custgroup',
        'sbv21_sector_legal_description',
        'branch_parent_name',
        'branch_name',
        'cif',
        'full_name',
        'limit_reference_3',
        'gl',
        'ld_no',
        'link_reference',
        't_bpm_disp_id',
        'loan_classification',
        'vmb_ln_class',
        'value_date',
        'mature_date',
        'term',
        'draw_down_amt',
        'total_amount',
        'total_amount_eq',
        'currency',
        'interate_rate',
        'interest_spread',
        'int_rate_type',
        'interest_key',
        'frequency',
        'pr_day',
        'in_day',
        'category_description',
        'loan_subproduct',
        'main_purpose_name',
        'industry_lev1',
        'account_officer_name',
        'promotion_name',
        'ld_aprv_level',
        'dao_2019',
        't_ocb_outof_area',
    ]

    col_names = {
        'custgroup': 'cust_group',
        'sbv21_sector_legal_description': 'cust_type',
        'branch_parent_name': 'parent_branch',
        'branch_name': 'branch',
        'full_name': 'cust_name',
        'limit_reference_3': 'limit',
        'ld_no': 'id',
        'link_reference': 'old_id',
        't_bpm_disp_id': 'bpm_id',
        'loan_classification': 'class',
        'vmb_ln_class': 'manual_class',
        'draw_down_amt': 'draw_down_amt',
        'total_amount': 'amt',
        'total_amount_eq': 'amt_eq',
        'currency': 'ccy',
        'interate_rate': 'interest',
        'int_rate_type': 'interest_type',
        'frequency': 'interest_freq',
        'category_description': 'product',
        'loan_subproduct': 'sub_product',
        'main_purpose_name': 'purpose',
        'industry_lev1': 'industry',
        'account_officer_name': 'officer',
        'promotion_name': 'promotion',
        'ld_aprv_level': 'approver',
        'dao_2019': 'ncov',
        't_ocb_outof_area': 'out_of_area',
    }

    data_on_sheet = data_on_sheet[cols]
    data_on_sheet.rename(col_names, axis='columns', inplace=True)

    print('Checking for missing values ...')
    cols = ['data_date', 'cust_group', 'cust_type', 'parent_branch', 'branch', 'cif', 'id', 'ccy', 'product', 'gl']
    tmp = data_on_sheet[cols].isna().any()
    if tmp.any():
        print(tmp[tmp])
        raise Exception('Please fill in missing values and retry')

    print('Transforming on balance sheet data ...')
    data_on_sheet['type'] = 'LN'

    data_on_sheet['data_date'] = pd.to_datetime(data_on_sheet['data_date'], format='%Y%m%d')
    data_on_sheet['value_date'] = pd.to_datetime(data_on_sheet['value_date'], format='%Y%m%d')
    data_on_sheet['mature_date'] = pd.to_datetime(data_on_sheet['mature_date'], format='%Y%m%d')

    data_on_sheet['class'].fillna(1, inplace=True)

    data_on_sheet['pr_day'].fillna(0, inplace=True)
    data_on_sheet['in_day'].fillna(0, inplace=True)
    data_on_sheet['pd_day'] = data_on_sheet[['pr_day', 'in_day']].max(axis='columns')
    data_on_sheet.drop(['pr_day', 'in_day'], axis='columns', inplace=True)
    data_on_sheet['class_by_day'] = pd.cut(data_on_sheet['pd_day'], bins=(-np.inf, 9, 90, 180, 360, np.inf), labels=(1, 2, 3, 4, 5))

    data_on_sheet['cust_group'] = data_on_sheet['cust_group'].replace(param_cust_group['cust_group'])
    if not data_on_sheet['cust_group'].isin(param_cust_group['cust_group'].values()).all():
        raise Exception('Unknow cust group. Please update params file')

    cond = data_on_sheet['old_id'].str.slice(0,2) == 'LD'
    data_on_sheet.loc[~cond, 'old_id'] = data_on_sheet.loc[~cond, 'id']
    tmp = data_on_sheet.groupby('old_id', as_index=False).agg({'value_date': np.min})
    tmp.columns = ['old_id', 'orig_value_date']
    data_on_sheet = data_on_sheet.merge(tmp, on='old_id')

    data_on_sheet['interest_type'] = data_on_sheet['interest_type'].str.slice(4)
    data_on_sheet['interest_key'] = data_on_sheet['interest_key'].str.extract(r'^(\d*)')
    data_on_sheet['sub_product'] = data_on_sheet['sub_product'].str.slice(6)
    data_on_sheet['industry'] = data_on_sheet['industry'].str.slice(5).str.lower()
    data_on_sheet['approver'] = data_on_sheet['approver'].str.slice(4)
    data_on_sheet['out_of_area'] = data_on_sheet['out_of_area'].str.extract(r'-\s*(.*)')

    prod = 'CHO VAY SINH HOAT TIEU DUNG'
    sub_prods = [
        'Vay tiêu dùng nhanh thế chấp BĐS dành cho KHCN',
        'SP vay siêu tốc thế chấp BĐS'
    ]
    cond = (data_on_sheet['product'] == prod) & (data_on_sheet['sub_product'].isin(sub_prods))
    data_on_sheet.loc[cond, 'product'] = prod + ' (NHANH)'
    print('Warning, please check the product "' + prod + ' (NHANH)" again')

    data_on_sheet = data_on_sheet.merge(jica_rdf_vnsat, on='id', how='left')

    data_off_sheet.columns = data_off_sheet.columns.str.lower()

    print('Filtering off balance sheet data ...')
    data_off_sheet = data_off_sheet[data_off_sheet['macn_ql'].isin(parent_branchs)]
    data_off_sheet = data_off_sheet[~data_off_sheet['macn'].isin(excluded_branchs)]
    print('{} row(s) remain'.format(data_off_sheet.shape[0]))

    cols = [
        'data_date',
        'custgroup',
        ' sbv21_sector_legal_description', # Beginning with space !
        'tencn_ql',
        'tencn',
        'cif',
        'ten',
        'ma_hm_c3',
        'gl',
        'tieukhoan',
        'startdate',
        'enddate',
        'lc_expiry_date',
        'ngoaite1',
        'noite',
        'ngoaite',
        'category',
        'industry_lev1',
        't_fut_real_est',
    ]

    col_names = {
        'custgroup': 'cust_group',
        ' sbv21_sector_legal_description': 'cust_type',
        'tencn_ql': 'parent_branch',
        'tencn': 'branch',
        'ten': 'cust_name',
        'ma_hm_c3': 'limit',
        'tieukhoan': 'id',
        'startdate': 'value_date',
        'enddate': 'mature_date',
        'ngoaite1': 'amt',
        'noite': 'amt_eq',
        'ngoaite': 'ccy',
        'category': 'product',
        'industry_lev1': 'industry',
        't_fut_real_est': 'future_re',
    }

    data_off_sheet = data_off_sheet[cols]
    data_off_sheet.rename(col_names, axis='columns', inplace=True)

    print('Checking for missing values ...')
    cols = ['data_date', 'cust_group', 'cust_type', 'parent_branch', 'branch', 'cif', 'id', 'ccy', 'product', 'gl']
    tmp = data_off_sheet[cols].isna().any()
    if tmp.any():
        print(tmp[tmp])
        raise Exception('Please fill in missing values and retry')

    print('Transforming on balance sheet data ...')
    data_off_sheet['type'] = 'BL/LC'

    data_off_sheet['old_id'] = data_off_sheet['id']
    data_off_sheet['class'] = 1

    data_off_sheet['data_date'] = pd.to_datetime(data_off_sheet['data_date'], format='%Y%m%d')
    data_off_sheet['value_date'] = pd.to_datetime(data_off_sheet['value_date'], format='%Y%m%d')
    data_off_sheet['orig_value_date'] = data_off_sheet['value_date']
    data_off_sheet['mature_date'] = pd.to_datetime(data_off_sheet['mature_date'], format='%Y%m%d')
    data_off_sheet['lc_expiry_date'] = pd.to_datetime(data_off_sheet['lc_expiry_date'], format='%Y%m%d')
    cond = data_off_sheet['lc_expiry_date'].isna()
    data_off_sheet.loc[~cond, 'mature_date'] = data_off_sheet.loc[~cond, 'lc_expiry_date']
    data_off_sheet.drop(['lc_expiry_date'], axis='columns', inplace=True)

    data_off_sheet['cust_group'] = data_off_sheet['cust_group'].replace(param_cust_group['cust_group'])
    if not data_off_sheet['cust_group'].isin(param_cust_group['cust_group'].values()).all():
        raise Exception('Unknow cust group. Please update params file')

    data_off_sheet['product'] = data_off_sheet['product'].replace(param_product['product'])
    if not data_off_sheet['product'].isin(param_product['product'].values()).all():
        raise Exception('Unknow product. Please update params file')

    data_off_sheet['industry'] = data_off_sheet['industry'].str.slice(5).str.lower()

    print('Merging ...')
    master = pd.concat([data_on_sheet, data_off_sheet], ignore_index=True)

    print('Calculating real class')
    master = master.merge(cic, on='cif', how='left')
    master['real_class'] = master[['class', 'class_by_day', 'cic_class']].fillna(1).max(axis='columns')
    tmp = master.groupby('cif', as_index=False).agg({'real_class': np.max})
    master.drop(['real_class'], axis='columns', inplace=True)
    master = master.merge(tmp, on='cif', how='left')

    master = master[[
        'data_date',
        'cust_group',
        'cust_type',
        'parent_branch',
        'branch',
        'cif',
        'cust_name',
        'type',
        'gl',
        'limit',
        'id',
        'old_id',
        'bpm_id',
        'value_date',
        'orig_value_date',
        'mature_date',
        'term',
        'ccy',
        'draw_down_amt',
        'amt',
        'amt_eq',
        'class',
        'pd_day',
        'class_by_day',
        'cic_class',
        'real_class',
        'manual_class',
        'interest',
        'interest_spread',
        'interest_type',
        'interest_key',
        'interest_freq',
        'product',
        'sub_product',
        'purpose',
        'promotion',
        'future_re',
        'industry',
        'officer',
        'approver',
        'out_of_area',
        'ncov',
        'sof',
    ]]

    print('DONE')

    return master