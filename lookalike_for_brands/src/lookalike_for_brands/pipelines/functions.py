import collections
import datetime
from datetime import date, timedelta
import itertools
import logging
import pickle
from collections import Counter
from datetime import datetime
from typing import Dict, Tuple
import magpie.sql_utils as su
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from imblearn.under_sampling import RandomUnderSampler
from scipy import stats
from sklearn.model_selection import StratifiedKFold, cross_val_predict
import os
import sys
import time
from sklearn.calibration import CalibratedClassifierCV
from progress.bar import IncrementalBar
from functools import reduce
from operator import concat

from lookalike_for_brands.pipelines.sql_utils import *

# from sql_utils import *

# plt.rcParams["figure.figsize"] = (20,10)
plt.style.use("seaborn")
import warnings

# plotly imports
from dateutil.relativedelta import *
import logging

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from lightgbm import LGBMClassifier
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

warnings.simplefilter("ignore")

# logging.basicConfig(filename='execution.log',filemode='w',format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
# datefmt='%H:%M:%S',level=logging.DEBUG)

site_format_eng = {"МД": "MD", "МК": "MK", "БФ": "BF", "ПР": "PR"}


def change_formatting(a):
    """
    change_formatting changes the format to tuple of 2 or more elements

    Args:
        a (string or list/array): string or list to be reformatted to tuple
        for sql scripts
    Returns:
        tuple: reformatted to tuple list or string
    """
    if type(a) == list or type(a) == np.ndarray:
        if len(a) > 1:
            a = tuple(a)
        else:
            a = tuple([a[0], a[0]])
    else:
        a = tuple([a, a])
    return a


def list_to_str(a):
    """
    list_to_str reformats list to string
    Args:
        a (list):list for reformatting

    Returns:
        string: result string
    """
    if type(a) == list or type(a) == np.ndarray:
        b = a[0]
        try:
            for e in a[1:]:
                b += ", " + e
        except:
            pass
    else:
        b = a
    return b


column_types = {
    "contact_id": "int32",
    "rto_cat": "float32",
    "prd_cnt_cat": "int8",
    "trn_cnt_cat": "int8",
    "rto_brand": "float32",
    "prd_cnt_brand": "int8",
    "trn_cnt_brand": "int8",
    " rto_dist": "float32",
    "prd_cnt_dist": "int8",
    "trn_dist": "int8",
    "trn_period": "int8",
    "days_since_last_trn": "int8",
    "max_days_btw": "int8",
    "avg_days_btw": "float32",
    "trn_days": "int8",
    "rto": "float32",
    "avg_daily_bill": "float32",
    "max_daily_bill": "float32",
    "min_daily_bill": "float32",
    "trn_cnt": "int8",
    "prd_cnt": "int8",
    "avg_daily_prd_cnt": "float32",
    "max_daily_prd_cnt": "int8",
    "min_daily_prd_cnt": "int8",
    "gender": "int8",
    "age": "int8",
}


def reformat_type(data):
    assert isinstance(
        data, pd.DataFrame
    ), "data needs to be a pandas.DataFrame"
    for c in data.columns:
        if c not in column_types.keys():
            # data[c] = data[c].astype("int8")
            pass
        else:
            data[c] = data[c].astype(column_types[c])
    return data


def check_data(data):
    for col in data.columns:
        data = data[data[col] >= 0]
    return data


def collect_customer_trn(parameters: Dict):
    """
    collect_customer_trn collects customer transaction for target
    Args:
        parameters (Dict): dictionary with parameters

    Returns:

    """
    begin_cus_dt = parameters["begin_cus_dt"]
    end_cus_dt = parameters["end_cus_dt"]
    cat_level = parameters["cat_level"]
    cat_name = parameters["cat_name"]
    site_format = parameters["site_format"]
    cat_name = change_formatting(reduce(concat,cat_name))  # [0]
    # print('Collecting transations in categories: ',cat_id)
    if site_format:
        site_format = "and frmt in {}".format(change_formatting(site_format))
    else:
        site_format = " and frmt in ('МК','МД','БФ') "
    try:
        su.truncate_table_gp("CVM_SBX.MT_CVM_LOOKALIKE_TRN_TARGET")
        su.drop_table_gp("CVM_SBX.MT_CVM_LOOKALIKE_TRN_TARGET")
    except:
        pass
    sql_create = query_create_tbl_collect_customer_trn(
        "CVM_SBX.MT_CVM_LOOKALIKE_TRN_TARGET"
    )
    su.execute_custom_query_gp(sql_create)
    logging.info(
            "Inserting into CVM_SBX.MT_CVM_LOOKALIKE_TRN_TARGET"
        )
    bar = IncrementalBar('Countdown', max = 10)
    for cus in range(10):
        sql = query_collect_customer_trn(
        "CVM_SBX.MT_CVM_LOOKALIKE_TRN_TARGET",
        begin_cus_dt,
        end_cus_dt,
        site_format,
        cat_level,
        cat_name,
        cus
        )
        bar.next()
        time.sleep(1)
        su.execute_custom_query_gp(sql)
    bar.finish()
    return


def collect_customer_trn1(parameters):
    begin_cus_dt = parameters["begin_cus_dt"]
    end_cus_dt = parameters["end_cus_dt"]
    site_format = parameters["site_format"]
    if site_format:
        site_format = "and frmt in {}".format(change_formatting(site_format))
    else:
        site_format = " and frmt IN ('МК','МД','БФ') "
    try:
        su.truncate_table_gp("CVM_SBX.MT_CVM_LOOKALIKE_TRN_TARGET")
        su.drop_table_gp("CVM_SBX.MT_CVM_LOOKALIKE_TRN_TARGET")
    except:
        pass
    sql_create = query_create_tbl_collect_customer_trn(
        "CVM_SBX.MT_CVM_LOOKALIKE_TRN_TARGET"
    )
    su.execute_custom_query_gp(sql_create)
    print(
            "Inserting into CVM_SBX.MT_CVM_LOOKALIKE_TRN_TARGET"
        )
    bar = IncrementalBar('Countdown', max = 10)
    for cus in range(10):
        sql = query_collect_customer_trn1(
        "CVM_SBX.MT_CVM_LOOKALIKE_TRN_TARGET",
        begin_cus_dt,
        end_cus_dt,
        site_format,
        cus
        )
        bar.next()
        time.sleep(1)
        su.execute_custom_query_gp(sql)
    bar.finish()
    return


def collect_customers(
    cat_level,
    cat_name,
    brand_col,
    brand_name,
    dist_flag,
    dist_col=False,
    dist_name=False,
):
    brand_name = change_formatting(brand_name)
    cat_name = change_formatting(cat_name)
    try:
        su.truncate_table_gp("CVM_SBX.MT_CVM_LOOKALIKE_CUST")
        su.drop_table_gp("CVM_SBX.MT_CVM_LOOKALIKE_CUST")
    except:
        pass
    sql_create = query_create_tbl_collect_customers(
        "CVM_SBX.MT_CVM_LOOKALIKE_CUST"
    )
    su.execute_custom_query_gp(sql_create)
    print("CVM_SBX.MT_CVM_LOOKALIKE_CUST created")
    custom_query = ""
    if dist_name and dist_flag:
        custom_query += """,SUM(CASE WHEN {0}='{1}' AND {2} IN {3} THEN summ ELSE 0 END) AS rto_dist
        ,COUNT(CASE WHEN {0}='{1}' AND {2} IN {3} THEN article_id ELSE NULL END) AS prd_cnt_dist
        ,COUNT(DISTINCT CASE WHEN {0}='{1}' AND {2} IN {3} THEN cheque_pk ELSE NULL END) AS trn_cnt_dist""".format(
            dist_col, dist_name, brand_col, brand_name
        )
    sql = query_collect_customers(
        "CVM_SBX.MT_CVM_LOOKALIKE_CUST",
        brand_col,
        brand_name,
        custom_query,
        cat_level,
        cat_name,
    )
    su.execute_custom_query_gp(sql)
    print("CVM_SBX.MT_CVM_LOOKALIKE_CUST inserted")
    cust = su.get_data_gp("CVM_SBX.MT_CVM_LOOKALIKE_CUST")
    print("CVM_SBX.MT_CVM_LOOKALIKE_CUST downloaded")
    cust = reformat_type(cust)
    cust = check_data(cust)
    print(cust.columns)
    print(cust.head(5))
    print(cust.info())
    cust["rto_cat_brand_ratio"] = np.where(
        cust["rto_cat"] > 0, cust["rto_brand"] / cust["rto_cat"], 0
    )
    cust["prd_cnt_cat_brand_ratio"] = np.where(
        cust["prd_cnt_cat"] > 0, cust["prd_cnt_brand"] / cust["prd_cnt_cat"], 0
    )
    cust["trn_cnt_cat_brand_ratio"] = np.where(
        cust["trn_cnt_cat"] > 0, cust["trn_cnt_brand"] / cust["trn_cnt_cat"], 0
    )
    if dist_flag and dist_name:
        cust["rto_dist_ratio"] = np.where(
            cust["rto_cat"] > 0, cust["rto_dist"] / cust["rto_cat"], 0
        )
        cust["prd_cnt_dist_ratio"] = np.where(
            cust["prd_cnt_cat"] > 0,
            cust["prd_cnt_dist"] / cust["prd_cnt_cat"],
            0,
        )
        cust["trn_cnt_dist_ratio"] = np.where(
            cust["trn_cnt_cat"] > 0,
            cust["trn_cnt_dist"] / cust["trn_cnt_cat"],
            0,
        )
    return cust


def collect_customers1(cat_level, cat_name):
    try:
        su.truncate_table_gp("CVM_SBX.MT_CVM_LOOKALIKE_CUST")
        su.drop_table_gp("CVM_SBX.MT_CVM_LOOKALIKE_CUST")
    except:
        pass
    sql_create = query_create_tbl_collect_customers(
        "CVM_SBX.MT_CVM_LOOKALIKE_CUST"
    )
    su.execute_custom_query_gp(sql_create)
    print("CVM_SBX.MT_CVM_LOOKALIKE_CUST created 1")
    sql = query_collect_customers1(
        "CVM_SBX.MT_CVM_LOOKALIKE_CUST", cat_level, cat_name
    )
    su.execute_custom_query_gp(sql)
    print("CVM_SBX.MT_CVM_LOOKALIKE_CUST inserted 1")
    cust = su.get_data_gp("CVM_SBX.MT_CVM_LOOKALIKE_CUST")
    print("CVM_SBX.MT_CVM_LOOKALIKE_CUST downloaded 1")
    cust = reformat_type(cust)
    cust = check_data(cust)
    print(cust.columns)
    print(cust.head(5))
    print(cust.info())
    cust["rto_cat_ratio"] = np.where(
        cust["rto"] != 0, cust["rto_cat"] / cust["rto"], 0
    )
    cust["prd_cnt_cat_ratio"] = np.where(
        cust["prd_cnt"] != 0, cust["prd_cnt_cat"] / cust["prd_cnt"], 0
    )
    cust["trn_cnt_cat_ratio"] = np.where(
        cust["trn_cnt"] != 0, cust["trn_cnt_cat"] / cust["trn_cnt"], 0
    )
    return cust


def collect_cust_data(calc_dt, cust_table):
    sql = query_cust_data(calc_dt, cust_table)
    cust_data = su.get_data_gp(sql)
    return cust_data


def collect_transprod(begin_dt, end_dt, cust_tbl, cat_level, cat_name):
    cat_name = change_formatting(cat_name)
    sql = query_get_cat(0, cat_level, cat_name)
    cat20_name = su.get_data_gp(sql)
    cat20_name_tpl = tuple_from_list(
        cat20_name["art_grp_lvl_0_name"].to_list()
    )
    try:
        su.truncate_table_gp("CVM_SBX.MT_CVM_LOOKALIKE_TRNPROD")
        su.drop_table_gp("CVM_SBX.MT_CVM_LOOKALIKE_TRNPROD")
    except:
        pass
    sql_create = query_create_tbl_collect_transprod(
        "CVM_SBX.MT_CVM_LOOKALIKE_TRNPROD"
    )
    su.execute_custom_query_gp(sql_create)
    print(
            "Inserting into CVM_SBX.MT_CVM_LOOKALIKE_TRNPROD"
        )
    bar = IncrementalBar('Countdown', max = 50)
    for cus in range(50):
        sql = query_collect_transprod(
            "CVM_SBX.MT_CVM_LOOKALIKE_TRNPROD",
            cust_tbl,
            begin_dt,
            end_dt,
            cat20_name_tpl,
            cus
        )
        bar.next()
        time.sleep(1)
        su.execute_custom_query_gp(sql)
    bar.finish()
    return


def collect_trn(begin_dt, end_dt, cust_tbl):
    try:
        su.truncate_table_gp("CVM_SBX.MT_CVM_LOOKALIKE_TRN")
        su.drop_table_gp("CVM_SBX.MT_CVM_LOOKALIKE_TRN")
    except:
        pass
    sql_create = query_create_tbl_collect_trn("CVM_SBX.MT_CVM_LOOKALIKE_TRN")
    su.execute_custom_query_gp(sql_create)
    print(
            "Inserting into CVM_SBX.MT_CVM_LOOKALIKE_TRN"
        )
    bar = IncrementalBar('Countdown', max = 50)
    for cus in range(50):
        sql = query_collect_trn(
            "CVM_SBX.MT_CVM_LOOKALIKE_TRN", cust_tbl, begin_dt, end_dt, cus
        )
        #print(f"Insert CVM_SBX.MT_CVM_LOOKALIKE_TRN for {cus+1} part of 50")
        bar.next()
        time.sleep(1)
        su.execute_custom_query_gp(sql)
    bar.finish()
    return


def calc_feat(
    calc_dt, begin_dt, site_format=False, cat_level=False, cat_name=False
):
    cat_name = change_formatting(cat_name)
    custom_query = ""
    if site_format:
        custom_query += """ AND frmt='{0}' """.format(site_format)
    if cat_level:
        custom_query += """ AND art_grp_lvl_{0}_name IN {1} """.format(
            cat_level, cat_name
        )
    sql = query_calc_feat(
        "CVM_SBX.MT_CVM_LOOKALIKE_TRNPROD", calc_dt, begin_dt, custom_query
    )
    df = su.get_data_gp(sql)
    df = df.fillna(0)
    df = reformat_type(df)
    df = check_data(df)
    return df


def calc_feat_trn(calc_dt, begin_dt, site_format=False):
    custom_query = ""
    if site_format:
        custom_query += """ AND frmt='{0}' """.format(site_format)
    sql = query_calc_feat_trn(
        "CVM_SBX.MT_CVM_LOOKALIKE_TRN", calc_dt, begin_dt, custom_query
    )
    df = su.get_data_gp(sql)
    df = df.fillna(0)
    df = reformat_type(df)
    df = check_data(df)
    return df


def calc_feat_price(
    begin_dt,
    site_format=False,
    cat_level=False,
    cat_name=False,
    price_level=False,
):
    cat_name = change_formatting(cat_name)
    custom_query = ""
    if site_format:
        custom_query += """ AND frmt='{0}' """.format(site_format)
    if cat_level:
        custom_query += """ AND art_grp_lvl_{0}_name IN {1} """.format(
            cat_level, cat_name
        )
    if price_level:
        custom_query += """ AND direction={0} """.format(price_level)
    sql = query_calc_feat_price(
        "CVM_SBX.MT_CVM_LOOKALIKE_TRNPROD", begin_dt, custom_query
    )
    df = su.get_data_gp(sql)
    df = reformat_type(df)
    df = check_data(df)
    return df


# General features
def collect_general_features(begin_dates, calc_dt, end_dt):
    for date in begin_dates:
        months = int(
            int(
                (
                    datetime.strptime(str(end_dt), "%Y-%m-%d")
                    - datetime.strptime(str(date), "%Y-%m-%d")
                ).days
            )
            / 28
        )
        gen_feat0 = calc_feat_trn(calc_dt, date)
        try:
            gen_feat = pd.concat([gen_feat, gen_feat0])
            del gen_feat0
        except:
            gen_feat = gen_feat0.copy()
            del gen_feat0
        gen_feat.columns = ["contact_id"] + [
            c + "_" + str(months) + "m"
            for c in gen_feat.columns
            if c != "contact_id"
        ]
        try:
            gen_df = gen_df.merge(
                gen_feat,
                right_on="contact_id",
                left_on="contact_id",
                how="left",
            ).fillna(0)
        except:
            gen_df = gen_feat.copy()
        print(gen_df.shape)
        del gen_feat
    return gen_df


# Features by format
def collect_format_features(site_formats, begin_dates, calc_dt, end_dt):
    for f in site_formats:
        for date in begin_dates:
            months = int(
                int(
                    (
                        datetime.strptime(str(end_dt), "%Y-%m-%d")
                        - datetime.strptime(str(date), "%Y-%m-%d")
                    ).days
                )
                / 28
            )
            format_feat0 = calc_feat_trn(
                calc_dt=calc_dt, begin_dt=date, site_format=f
            )
            try:
                format_feat = pd.concat([format_feat, format_feat0])
                del format_feat0
            except:
                format_feat = format_feat0.copy()
                del format_feat0
        format_feat.columns = ["contact_id"] + [
            c + "_" + site_format_eng[f] + "_" + str(months) + "m"
            for c in format_feat.columns
            if c != "contact_id"
        ]
        try:
            format_df = format_df.merge(
                format_feat,
                right_on="contact_id",
                left_on="contact_id",
                how="outer",
            ).fillna(0)
        except:
            format_df = format_feat.copy()
        print(format_df.shape)
        del format_feat
    return format_df


# Features by category
def collect_cat_features(
    cat_levels, cat_level, cat_name, begin_dates, calc_dt, end_dt
):
    cat_level_names = []
    cat_name = change_formatting(cat_name)
    for level in cat_levels:
        sql = query_get_cat(level, cat_level, cat_name)
        cat_level_name = su.get_data_gp(sql)
        cat_level_names.append(
            cat_level_name[f"art_grp_lvl_{level}_name"].values[0]
        )
        del cat_level_name
    for level, c_name in zip(cat_levels, cat_level_names):
        for date in begin_dates:
            months = int(
                int(
                    (
                        datetime.strptime(str(end_dt), "%Y-%m-%d")
                        - datetime.strptime(str(date), "%Y-%m-%d")
                    ).days
                )
                / 28
            )
            cat_feat0 = calc_feat(
                calc_dt=calc_dt,
                begin_dt=date,
                cat_level=level,
                cat_name=c_name,
            )
            try:
                cat_feat = pd.concat([cat_feat, cat_feat0])
                del cat_feat0
            except:
                cat_feat = cat_feat0.copy()
                del cat_feat0
        cat_feat.columns = ["contact_id"] + [
            c + "_" + "gr" + str(level) + "_" + str(months) + "m"
            for c in cat_feat.columns
            if c != "contact_id"
        ]
        try:
            cat_df = cat_df.merge(
                cat_feat,
                right_on="contact_id",
                left_on="contact_id",
                how="outer",
            ).fillna(0)
        except:
            cat_df = cat_feat.copy()
        print(cat_df.shape)
        del cat_feat
    return cat_df


# Features by price segment
def collect_price_features(
    cat_levels, price_levels, cat_level, cat_name, begin_dates, end_dt
):
    cat_level_names = []
    cat_name = change_formatting(cat_name)
    for level in cat_levels:
        sql = query_get_cat(level, cat_level, cat_name)
        cat_level_name = su.get_data_gp(sql)
        cat_level_names.append(
            cat_level_name["art_grp_lvl_{0}_name".format(level)].values[0]
        )
        del cat_level_name
    for p_level in price_levels:
        for level, c_name in zip(cat_levels, cat_level_names):
            for date in begin_dates:
                months = int(
                    int(
                        (
                            datetime.strptime(str(end_dt), "%Y-%m-%d")
                            - datetime.strptime(str(date), "%Y-%m-%d")
                        ).days
                    )
                    / 28
                )
                cat_feat0 = calc_feat_price(
                    begin_dt=date,
                    cat_level=level,
                    cat_name=c_name,
                    price_level=p_level,
                )
                try:
                    cat_feat = pd.concat([cat_feat, cat_feat0])
                    del cat_feat0
                except:
                    cat_feat = cat_feat0.copy()
                    del cat_feat0
            cat_feat.columns = ["contact_id"] + [
                c
                + "_"
                + "gr"
                + str(level)
                + "_"
                + str(p_level)
                + "_"
                + str(months)
                + "m"
                for c in cat_feat.columns
                if c != "contact_id"
            ]
            try:
                cat_df = cat_df.merge(
                    cat_feat,
                    right_on="contact_id",
                    left_on="contact_id",
                    how="outer",
                ).fillna(0)
            except:
                cat_df = cat_feat.copy()
            print(cat_df.shape)
            del cat_feat
    return cat_df


def collect_dna(cust_tbl):
    sql = query_collect_cust_dna(cust_tbl)
    dna_df = su.get_data_gp(sql)
    dna_df = dna_df.fillna(dna_df.max(axis=0) + 1)
    return dna_df


def write_list(a_list, filename):
    with open(filename, "wb") as fp:
        pickle.dump(a_list, fp)
        # print('Done writing list into a binary file')


def read_list(filename):
    with open(filename, "rb") as fp:
        n_list = pickle.load(fp)
        return n_list


def process_df(df_fin, cat_name, train=True):
    if train:
        regions_cnt = df_fin.groupby(["region"])[["contact_id"]].count()
        summary_df = (
            regions_cnt.sort_values(by="contact_id", ascending=False)
            .reset_index(drop=False)
            .head(n=100)
        )
        summary_df.rename(
            columns={
                summary_df.columns[0]: "region",
                summary_df.columns[1]: "region_count",
            },
            inplace=True,
        )
        total_group_count = summary_df.region_count.values.sum()
        summary_df["region_perc"] = (
            summary_df["region_count"] / total_group_count
        )
        summary_df["total_perc"] = summary_df.region_perc.cumsum()
        regions_flt = list(summary_df[(summary_df.total_perc <= 80)].region)
        region_dict = {}
        i = 0
        for reg in regions_flt:
            region_dict[reg] = i
            i += 1
        filename = "data/04_feature/regions{}.pkl".format(cat_name).strip("[]").replace(' ','_')
        write_list(region_dict, filename)
    else:
        filename = "data/04_feature/regions{}.pkl".format(cat_name).strip("[]").replace(' ','_')
        region_dict = read_list(filename)
        i = len(region_dict)
    df_fin["region"] = df_fin.region.map(region_dict).fillna(i)
    df_fin = df_fin.drop_duplicates()
    return df_fin


def train_eval_pipeline(clf, X, y):
    # clf = make_pipeline(model)
    cv = StratifiedKFold(n_splits=2, random_state=42, shuffle=True)
    # print("Calculating cross val scores")
    # logging.info('Calculating cross val scores')
    # scores = cross_val_score(clf, X, y["TARGET"], scoring="roc_auc", cv=cv, n_jobs=None)
    # print("Average roc_auc:", scores.mean())
    # logging.info('Average roc_auc: {}'.format(scores.mean()))
    # print("Calculating cross val probabilities")
    # logging.info("Calculating cross val probabilities")
    # print(y.head())
    # print(X.head())
    under_sampler = RandomUnderSampler(random_state=42)
    X_res, y_res = under_sampler.fit_resample(X, y)
    print(Counter(y_res["target"]))
    X_res.index = X.index[under_sampler.sample_indices_]
    y_res.index = y.index[under_sampler.sample_indices_]
    # print(X_res.head())
    X_leftover = X.drop(X_res.index, axis=0)
    # y_leftover=y[~y.index.isin([y_res.index])]
    # X_leftover = X[~X.index.isin([X_res.index])]
    print(X_leftover.shape)
    cross_val_flag = False
    if cross_val_flag:
        logging.info("Calculating cross val probabilities")
        cross_val_preds = cross_val_predict(
            clf, X_res, y_res["target"], cv=cv, method="predict_proba"
        )
        cross_val_preds = pd.Series(
            data=cross_val_preds[:, 1], index=y_res.index
        )
    test = False
    if test:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.20, random_state=27, stratify=y
        )

        clf.fit(X_train, y_train)
        prediction = clf.predict(X_test)
        conf_matrix(confusion_matrix(y_test, prediction), "Blues")
        print(classification_report(prediction, y_test))
        roc_auc(clf, X_test, y_test)
    else:
        logging.info("Fitting the model")
        X_train, X_calib, y_train, y_calib = train_test_split(
            X, y, test_size=0.20, random_state=27, stratify=y
        )
        clf.fit(X_train, y_train)
        calibrated_clf = CalibratedClassifierCV(clf, cv="prefit",method='isotonic')
        calibrated_clf.fit(X_calib, y_calib)
        if cross_val_flag:
            leftover_preds = clf.predict_proba(X_leftover)[:, 1]
            leftover_preds = pd.Series(
                data=leftover_preds, index=X_leftover.index
            )
            cross_val_preds = pd.concat(
                [cross_val_preds, leftover_preds], axis=0
            )
        else:
            logging.info("Calculating probabilities")
            cross_val_preds = clf.predict_proba(X)[:, 1]
            calib_preds = calibrated_clf.predict_proba(X)[:, 1]
            cross_val_preds = pd.Series(data=cross_val_preds, index=y.index)
            calib_preds = pd.Series(data=calib_preds, index=y.index)
    return [clf, cross_val_preds,calibrated_clf,calib_preds]


def roc_auc(clf, X_test, y_test):
    probs = clf.predict_proba(X_test)
    preds = probs[:, 1]
    fpr, tpr, threshold = metrics.roc_curve(y_test, preds)
    roc_auc = metrics.auc(fpr, tpr)
    print(roc_auc)
    plt.title("Receiver Operating Characteristic")
    plt.plot(fpr, tpr, "b", label="AUC = %0.2f" % roc_auc)
    plt.legend(loc="lower right")
    plt.plot([0, 1], [0, 1], "r--")
    plt.xlim([0, 1])
    plt.ylim([0, 1])
    plt.ylabel("True Positive Rate")
    plt.xlabel("False Positive Rate")
    plt.show()
    # plt.savefig('data/08_reporting/roc_curve.png')
    return roc_auc


def conf_matrix(cf_matrix, color):
    cf_matrix = cf_matrix
    group_names = ["True Neg", "False Pos", "False Neg", "True Pos"]
    group_counts = ["{0:0.0f}".format(value) for value in cf_matrix.flatten()]
    group_percentages = [
        "{0:.2%}".format(value)
        for value in cf_matrix.flatten() / np.sum(cf_matrix)
    ]
    labels = [
        f"{v1}\n{v2}\n{v3}"
        for v1, v2, v3 in zip(group_names, group_counts, group_percentages)
    ]
    labels = np.asarray(labels).reshape(2, 2)
    sns.heatmap(cf_matrix, annot=labels, fmt="", cmap=color)


def collect_all_data(train, collect_features, parameters):
    begin_dt = parameters["begin_dt"]
    begin_dt1 = parameters["begin_dt1"]
    end_dt = parameters["end_dt"]
    calc_dt = parameters["calc_dt"]
    cat_level = parameters["cat_level"]
    cat_name = parameters["cat_name"]
    site_formats = parameters["site_formats"]
    cat_levels = parameters["cat_levels"]

    if train:
        brand_col = parameters["brand_col"]
        brand_name = parameters["brand_name"]
        dist_flag = parameters["dist_flag"]
        dist_col = parameters["dist_col"]
        dist_name = parameters["dist_name"]
        threshold_variable = parameters["threshold_variable"]
        threshold_percentile = parameters["threshold_percentile"]
        # logging.info("Collecting customers")
        print("Collecting customers")
        if brand_name:
            logging.info("Mode: brand")
            cust = collect_customers(
                cat_level,
                cat_name,
                brand_col,
                brand_name,
                dist_flag,
                dist_col,
                dist_name,
            )
            print("collect_customers over")
            print(cust.shape)
        else:
            logging.info("Mode: category")
            cust = collect_customers1(cat_level, cat_name)
            print("collect_customers1 over")
            # Adding TARGET variable
        print(cust[cust[threshold_variable] > 0].shape)
        threshold_value = np.percentile(
            cust[cust[threshold_variable] > 0][threshold_variable],
            threshold_percentile,
        )
        logging.info(
            "Threshold value for {0} for {1} percentile: {2}".format(
                threshold_variable, threshold_percentile, threshold_value
            ))
        print("Threshold value for {0} for {1} percentile: {2}".format(
                threshold_variable, threshold_percentile, threshold_value
            )
        )
        cust["target"] = np.where(
            cust[threshold_variable] >= threshold_value, 1, 0
        )
        cust.columns = [column.lower() for column in cust.columns]
        # logging.info("Target distribution: {}".format(Counter(cust["target"])))
        print("Target distribution: {}".format(Counter(cust["target"])))
        target_df = cust[["contact_id", "target"]].copy()
        target_df = target_df.set_index("contact_id")
        # del cust
        cust_table = "CVM_SBX.MT_CVM_LOOKALIKE_CUST"
    else:
        if parameters["brand_name"]:
            cust_table = parameters["customer_table"]
            # cust_table = "(SELECT CUS_GID FROM {0} WHERE GROUP22='{1}') ".format(
            # parameters["customer_table"], cat_name
            # )
        else:
            cust_table = cust_table = parameters["customer_table"]
    if collect_features:
        print("Collecting customer data")
        # logging.info("Collecting customer data")
        cust_data = collect_cust_data(calc_dt, cust_table)
        print("Collecting transprod")
        # logging.info("Collecting transprod")
        collect_transprod(begin_dt, end_dt, cust_table, cat_level, cat_name)
        print("Collecting transactions")
        # logging.info("Collecting transactions")
        collect_trn(begin_dt, end_dt, cust_table)
        print("Collecting general features")
        # logging.info("Collecting general features")
        gen_df = collect_general_features(
            [begin_dt, begin_dt1], calc_dt, end_dt
        )
        print("Collecting format features")
        # logging.info("Collecting format features")
        # format_df = collect_format_features(
        # site_formats, [begin_dt, begin_dt1], calc_dt, end_dt, max_cus,
        # )
        format_df = collect_format_features(
            site_formats, [begin_dt1], calc_dt, end_dt
        )
        print("Collecting category features")
        # logging.info("Collecting category features")
        cat_df = collect_cat_features(
            cat_levels,
            cat_level,
            cat_name,
            # [begin_dt, begin_dt1],
            [begin_dt],
            calc_dt,
            end_dt,
        )
        # logging.info("Collecting price segment features")
        print("Collecting price segment features")
        price_levels = [1, 2, 3, 4]
        price_df = collect_price_features(
            cat_levels, price_levels, cat_level, cat_name, [begin_dt], end_dt
        )
        print("Collecting dna clusters")
        # logging.info("Collecting dna clusters")
        dna_df = collect_dna(cust_table)
        df_fin = cust_data.copy()
        del cust_data
        df_fin = df_fin.merge(
            gen_df, right_on="contact_id", left_on="contact_id", how="inner"
        )
        del gen_df
        df_fin = df_fin.merge(
            format_df, right_on="contact_id", left_on="contact_id", how="left"
        )
        del format_df
        df_fin = df_fin.merge(
            cat_df, right_on="contact_id", left_on="contact_id", how="left"
        )
        del cat_df
        df_fin = df_fin.merge(
            price_df, right_on="contact_id", left_on="contact_id", how="left"
        )
        del price_df
        df_fin = df_fin.merge(
            dna_df, right_on="contact_id", left_on="contact_id", how="left"
        ).fillna(0)
        del dna_df
        df_fin = process_df(df_fin, cat_name, train)
        df_fin.columns = [column.lower() for column in df_fin.columns]
        X = df_fin.set_index("contact_id")
        print(X.shape)
        print(X.head(5))
        if train:
            X = X.merge(target_df,right_index=True,left_index=True)[X.columns]
            target_df = target_df.loc[X.index]
            print(target_df.shape)
            return [X, target_df, cust]
        else:
            return [X]
    else:
        if train:
            return [target_df, cust]
        else:
            return [X]


# def create_output_table(output_table_name):
#     try:
#         su.truncate_table_gp(output_table_name)
#         su.drop_table_gp(output_table_name)
#     except:
#         pass
# sql = query_create_output_tbl(output_table_name)
# su.execute_custom_query_gp(sql)
# return


def clear_data_tables():
    for table in [
        "CVM_SBX.MT_CVM_LOOKALIKE_TRN",
        "CVM_SBX.MT_CVM_LOOKALIKE_TRNPROD",
        "CVM_SBX.MT_CVM_LOOKALIKE_TRN_TARGET",
        "CVM_SBX.MT_CVM_LOOKALIKE_CUST",
    ]:
        try:
            su.truncate_table_gp(table)
            su.drop_table_gp(table)
        except:
            pass
    return


def tuple_from_list(group_names_to_keep: list) -> tuple:
    """convert list in tuple

    Args:
        group_names_to_keep (list): list to convert

    Returns:
        tuple: _description_
    """
    if len(group_names_to_keep) == 1:
        tuple_names = (group_names_to_keep[0], group_names_to_keep[0])
    else:
        tuple_names = tuple(group_names_to_keep)
    return tuple_names


def create_loop_val_days(begin_dd: str, end_dd: str, freq="D"):
    begin_dd1 = datetime.strptime(begin_dd, "%Y-%m-%d")
    end_dd1 = datetime.strptime(end_dd, "%Y-%m-%d")
    dates_one = pd.date_range(begin_dd1, end_dd1, inclusive="left", freq=freq)
    dates_two = pd.date_range(begin_dd1, end_dd1, inclusive="right", freq=freq)
    return dates_one, dates_two
