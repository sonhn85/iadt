import numpy as np
import pandas as pd

def transform_4(master, coll_val_dist):
    print('Transforming ...')
    groupby = ['parent_branch', 'branch', 'cif']
    master_by_cif = master.groupby(groupby, as_index=False).agg({
        'data_date': 'first',
        'cust_group': 'first',
        'cust_type': 'first',
        'cust_name': 'first',
        'id': 'count',
        'orig_value_date': np.max,
        'amt_eq': np.sum,
        'class': np.max,
        'real_class': np.max,
    })

    tmp = master[groupby + ['limit']].drop_duplicates().groupby(groupby, as_index=False).count()
    master_by_cif = master_by_cif.merge(tmp, on=groupby, how='left')

    tmp = coll_val_dist[groupby + ['coll_id']].drop_duplicates().groupby(groupby, as_index=False).count()
    master_by_cif = master_by_cif.merge(tmp, on=groupby, how='left')

    cols = [
        'data_date',
        'parent_branch',
        'branch',
        'cust_group',
        'cust_type',
        'cif',
        'cust_name',
        'limit',
        'id',
        'coll_id',
        'orig_value_date',
        'amt_eq',
        'class',
        'real_class',
    ]

    master_by_cif[cols]

    cols = groupby + ['product', 'amt_eq']
    cond = master['type']=='LN'
    i = len(groupby)
    tmp = master.loc[cond, cols].pivot_table(index=groupby, values='amt_eq', columns='product', aggfunc=np.sum).reset_index()
    tmp['ln_amt_eq'] = tmp.loc[:, tmp.columns[i:]].sum(axis='columns')
    master_by_cif = master_by_cif.merge(tmp, on=groupby, how='left')

    tmp = master.loc[~cond, cols].pivot_table(index=groupby, values='amt_eq', columns='product', aggfunc=np.sum).reset_index()
    tmp['md_tf_amt_eq'] = tmp.loc[:, tmp.columns[i:]].sum(axis='columns')
    master_by_cif = master_by_cif.merge(tmp, on=groupby, how='left')

    tmp = coll_val_dist.pivot_table(index=groupby, values='amt_alloc', columns='coll_type', aggfunc=np.sum).reset_index()
    master_by_cif = master_by_cif.merge(tmp, on=groupby, how='left')

    tmp = master[groupby + ['officer', 'value_date']]
    cond = tmp['value_date'].isna() | tmp['officer'].isna()
    tmp = tmp[~cond]
    tmp = tmp.groupby(groupby, as_index=False).agg({'value_date': 'idxmax'})
    tmp.set_index('value_date', inplace=True)
    tmp['officer'] = master['officer']
    master_by_cif = master_by_cif.merge(tmp, on=groupby, how='left')

    tmp = master[groupby + ['approver', 'value_date']]
    cond = tmp['value_date'].isna() | tmp['approver'].isna()
    tmp = tmp[~cond]
    tmp = tmp.groupby(groupby, as_index=False).agg({'value_date': 'idxmax'})
    tmp.set_index('value_date', inplace=True)
    tmp['approver'] = master['approver']
    master_by_cif = master_by_cif.merge(tmp, on=groupby, how='left')

    #tmp = master[groupby + ['future_re']]
    #tmp = tmp[~tmp['future_re'].isna()].groupby(groupby, as_index=False).agg(lambda s: 'x')
    #master_by_cif = master_by_cif.merge(tmp, on=groupby, how='left')

    tmp = master[groupby + ['ncov']]
    tmp = tmp[~tmp['ncov'].isna()].groupby(groupby, as_index=False).agg(lambda s: 'x')
    if tmp.shape[0] > 0:
        master_by_cif = master_by_cif.merge(tmp, on=groupby, how='left')
    else:
        master_by_cif['ncov'] = pd.NA

    tmp = master[groupby + ['sof']]
    tmp = tmp[~tmp['sof'].isna()].drop_duplicates().groupby(groupby, as_index=False).agg(lambda s: s.str.cat(sep=', '))
    if tmp.shape[0] > 0:
        master_by_cif = master_by_cif.merge(tmp, on=groupby, how='left')
    else:
        master_by_cif['sof'] = pd.NA

    master_by_cif['pick'] = pd.NA

    print('DONE')

    return master_by_cif