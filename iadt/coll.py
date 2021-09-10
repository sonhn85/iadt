import pandas as pd
import numpy as np

def transform_2(collateral):

    print('Transforming ...')
    collateral.columns = collateral.columns.str.lower()

    cols = [
        'ngaylaysl',
        'tencn',
        'cif',
        'tenkhachhang',
        'ts_id',
        'ts_collateral_type',
        'ts_description',
        'ts_currency',
        'ts_gen_ledger_value',
        'ts_gen_ledger_value_eq',
        'ts_value_date',
        'ts_coll_status',
        'ts_application_id',
        'lkts_limit_reference',
        'lkts_percent_alloc',
        'val_agent',
        'reval_date',
        'reval_agent',
        'reval_amount',
        't_reval_next_date',
        'ts_coll_sec_id',
        'ts_imp_store_id',
        'ts_coll_ins_prod',
        'ts_coll_ins_comp',
        'ts_ins_amount',
        'ts_ins_start',
        'ts_ins_end',
        'ts_cluster_name',
    ]

    col_names = {
        'ngaylaysl': 'data_date',
        'tencn': 'branch',
        'cif': 'owner_cif',
        'tenkhachhang': 'owner_name',
        'ts_id': 'coll_id',
        'ts_collateral_type': 'coll_type',
        'ts_description': 'coll_desc',
        'ts_currency': 'coll_ccy',
        'ts_gen_ledger_value': 'value',
        'ts_gen_ledger_value_eq': 'value_eq',
        'ts_value_date': 'value_date',
        'ts_coll_status': 'status',
        'ts_application_id': 'deposit_ref',
        'lkts_limit_reference': 'limit',
        'lkts_percent_alloc': 'percent_alloc',
        't_reval_next_date': 'reval_next_date',
        'ts_coll_sec_id': 'security',
        'ts_imp_store_id': 'bpm_ref',
        'ts_coll_ins_prod': 'insurance_type',
        'ts_coll_ins_comp': 'insurance_company',
        'ts_ins_amount': 'insurance_amount',
        'ts_ins_start': 'insurance_start',
        'ts_ins_end': 'insurance_end',
        'ts_cluster_name': 'cluster',
    }

    collateral = collateral[cols].copy()
    collateral = collateral[cols]
    collateral.rename(col_names, axis='columns', inplace=True)
    collateral.set_index('coll_id', drop=True, inplace=True)

    print('DONE')

    return collateral

def transform_3(collateral, master, param_coll_type):

    print('Transforming ...')
    alloc_df = collateral['limit'].str.split('::').reset_index().explode('limit', ignore_index=True)

    alloc_df['cif'] = alloc_df['limit'].str.extract(r'^(\d*)')
    alloc_df['cif'] = pd.to_numeric(alloc_df['cif'])

    cond_by_cif = alloc_df['cif'].isin(master['cif'].astype(np.float64))
    keep_coll_id = alloc_df.loc[cond_by_cif, 'coll_id']
    cond_by_coll = alloc_df['coll_id'].isin(keep_coll_id)
    alloc_by_coll_df = alloc_df[cond_by_coll]
    alloc_by_cif_df = alloc_df[cond_by_cif]
    if alloc_by_coll_df.shape[0] > alloc_by_cif_df.shape[0]:
        print('Warning, collateral link to more than one branch:')
        cond = alloc_by_coll_df['cif'].isin(alloc_by_cif_df['cif'])
        print(alloc_by_coll_df[~cond])
    alloc_df = alloc_df.loc[cond_by_cif, ['limit', 'coll_id']]

    limit_df = master.groupby('limit', as_index=False).agg({'amt_eq': np.sum})

    alloc_df = alloc_df.merge(collateral[['value_eq']].reset_index(), on='coll_id', how='left')
    alloc_df = alloc_df.merge(limit_df, on='limit', how='left')

    tmp = alloc_df.groupby('coll_id', as_index=False).agg({'amt_eq': np.sum})
    tmp.columns = ['coll_id', 'total_amt_eq']
    alloc_df = alloc_df.merge(tmp, on='coll_id', how='left')
    alloc_df['pct_alloc'] = alloc_df['amt_eq']/alloc_df['total_amt_eq'] * 100
    alloc_df.dropna(subset=['pct_alloc'], inplace=True)
    alloc_df = alloc_df[~(alloc_df['pct_alloc'] == 0.0)]

    alloc_df['value_alloc'] = alloc_df['value_eq']*alloc_df['pct_alloc'] / 100

    alloc_df = alloc_df[[
        'coll_id',
        'limit',
        'value_alloc'
    ]]

    alloc_df = master[['parent_branch', 'cust_group', 'branch', 'cif', 'id', 'type', 'amt_eq', 'limit']].merge(alloc_df, on='limit')

    limit_df.columns = ['limit', 'total_amt_eq']
    alloc_df = alloc_df.merge(limit_df, on='limit', how='left')
    alloc_df['value_alloc'] = alloc_df['value_alloc']*alloc_df['amt_eq'] / alloc_df['total_amt_eq']

    alloc_df = alloc_df[[
        'parent_branch',
        'cust_group',
        'branch',
        'cif',
        'id',
        'type',
        'amt_eq',
        'limit',
        'coll_id',
        'value_alloc'
    ]]

    tmp = alloc_df.groupby('id', as_index=False).agg({'value_alloc': np.sum})
    tmp.columns = ['id', 'value_alloc_total']
    alloc_df = alloc_df.merge(tmp, on='id', how='left')
    alloc_df['amt_alloc'] = alloc_df['amt_eq']*alloc_df['value_alloc'] / alloc_df['value_alloc_total']
    alloc_df = alloc_df[[
        'parent_branch',
        'cust_group',
        'branch',
        'cif',
        'id',
        'type',
        'amt_alloc',
        'limit',
        'coll_id',
        'value_alloc'
    ]]

    print('Filtering collateral file ...')
    collateral.reset_index(inplace=True)
    collateral = collateral[collateral['coll_id'].isin(alloc_df['coll_id'])]
    print('{} row(s) remain'.format(collateral.shape[0]))

    collateral = collateral.copy()
    collateral['coll_type'] = collateral['coll_type'].replace(param_coll_type['coll_type'])

    alloc_df = alloc_df.merge(collateral[['coll_id', 'coll_type']], on='coll_id', how='left')
    cond = alloc_df['coll_type'].isin(param_coll_type['coll_type'].values())
    if not cond.all():
        print(alloc_df[~cond])
        raise Exception('Unknow collateral type. Please update params file')

    tmp = master[['parent_branch', 'cust_group', 'branch', 'cif', 'id', 'type', 'amt_eq', 'limit']]
    tmp = tmp[~tmp['id'].isin(alloc_df['id'])]
    tmp = tmp.rename({'amt_eq': 'amt_alloc'}, axis='columns')
    alloc_df = pd.concat([alloc_df, tmp], ignore_index=True)
    alloc_df = alloc_df.fillna({'coll_type': 'Không TSBĐ'})
        
    print('DONE')

    return collateral, alloc_df