"""
This is a boilerplate pipeline 'score_pipeline'
generated using Kedro 0.18.7
"""

from typing import Dict, Tuple
import pandas as pd
import os
import sys
from lookalike_for_brands.pipelines.functions import *
import magpie.sql_utils as su


def collect_score_data(parameters: Dict) -> Tuple:
    logging.basicConfig(
        filename="data/08_reporting/execution_data.log",
        filemode="w",
        format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        level=logging.DEBUG,
    )
    parameters1 = parameters.copy()
    data = {}
    if parameters["brand_name"]:
        for cat_name, brand_name in zip(
            parameters["cat_name"], parameters["brand_name"]
        ):
            logging.info(
                "Starting to collect training data for category: {0} brand: {1}".format(
                    cat_name, brand_name
                )
            )
            parameters1["cat_name"] = cat_name
            parameters1["brand_name"] = brand_name
            print("Parameters: ", parameters1)
            try:
                X = data["cat_" + list_to_str(cat_name).lower()]
                logging.info("No collection of features needed")
            except:
                logging.info("Starting to collect features")
                X = collect_all_data(
                    train=False, collect_features=True, parameters=parameters1
                )[0]
                logging.info("Finished collecting features")
                logging.info("Collected data shape {}".format(X.shape))
                data["cat_" + list_to_str(cat_name).lower()] = X
    else:
        for cat_name in parameters["cat_name"]:
            logging.info(
                "Starting to collect training data for category: {0}".format(
                    cat_name
                )
            )
            parameters1["cat_name"] = cat_name
            print("Parameters: ", parameters1)
            try:
                X = data["cat_" + list_to_str(cat_name).lower()]
                logging.info("No collection of features needed")
            except:
                logging.info("Starting to collect features")
                X = collect_all_data(
                    train=False, collect_features=True, parameters=parameters1
                )[0]
                logging.info("Finished collecting features")
                logging.info("Collected data shape {}".format(X.shape))
                data["cat_" + list_to_str(cat_name).lower()] = X
                print(data.head())
    logging.info("Starting to delete tables from the database")
    clear_data_tables()
    logging.info("Deleted tables from the database")
    return [data]


def remove_outliers(data: Dict, parameters: Dict) -> Dict:
    """
    remove_outliers remove outliers from dataframe

    Args:
        data (Dict): dictionary of initial dataframes
        parameters (Dict): statistics for evaluation

    Returns:
        Dict: dictionary of dataframes without ouliers
    """
    def_outliers_by = parameters["def_outliers_by"]
    statistics = parameters["outlier_statistics"]
    try:
        min_enrol = parameters["min_enrollment"]
    except:
        min_enrol = 90
    logging.info("Starting to remove outliers")
    data_f = dict()
    for cat_name, brand_name in zip(
        parameters["cat_name"], parameters["brand_name"]
    ):
        logging.info(
            "Starting to remove outliers for category: {0} brand: {1}".format(
                cat_name, brand_name
            )
        )
        # Take Log: RTO & ATV
        df = data["cat_" + list_to_str(cat_name).lower()]
        n_cust = df.shape[0]
        mask = 1 > 0
        if def_outliers_by == "iqr":
            # quantile: RTO & ATV
            for s in statistics:
                logging.info(s)
                if s[:15] == "enrollment_days":
                    mask = df[s] >= min_enrol
                    n_cust_after_filt_enrolment = df[df[s] >= min_enrol].shape[
                        0
                    ]
                elif s[:3] == "rto":
                    rto_q_25 = df[s].quantile(0.25)
                    rto_q_75 = df[s].quantile(0.75)
                    # RTO: get lower and upper bounds
                    rto_iqr = rto_q_75 - rto_q_25
                    rto_lower = round(rto_q_25 - 2 * rto_iqr)
                    rto_upper = round(rto_q_75 + 2 * rto_iqr)
                    rto_mask = (df[s] >= rto_lower) & (df[s] <= rto_upper)
                    mask = mask & rto_mask
                elif s[:14] == "avg_daily_bill":
                    atv_q_25 = df[s].quantile(0.25)
                    atv_q_75 = df[s].quantile(0.75)
                    # ATV: get lower and upper bounds
                    atv_iqr = atv_q_75 - atv_q_25
                    atv_lower = round(atv_q_25 - 2 * atv_iqr)
                    atv_upper = round(atv_q_75 + 2 * atv_iqr)
                    atv_mask = (df[s] >= atv_lower) & (df[s] <= atv_upper)
                    mask = mask & atv_mask
                else:
                    logging.info("Unknown statistics")
        elif def_outliers_by == "default":
            for s in statistics:
                if s[:15] == "enrollment_days":
                    mask = df[s] >= min_enrol
                    n_cust_after_filt_enrolment = df[df[s] >= min_enrol].shape[
                        0
                    ]
                elif s[:3] == "rto":
                    # RTO: get lower and upper bounds
                    df["rto_log"] = np.log(df[s])
                    rto_log_mean = df[df[s] > 0].rto_log.mean()
                    rto_log_std = df[df[s] > 0].rto_log.std(ddof=1)
                    rto_lower = round(np.exp(rto_log_mean - rto_log_std * 3))
                    rto_upper = round(np.exp(rto_log_mean + rto_log_std * 2))
                    rto_mask = (df[s] >= rto_lower) & (df[s] <= rto_upper)
                    logging.info(f"RTO bounds: ({rto_lower}, {rto_upper})")
                    mask = mask & rto_mask
                    df.drop(columns=["rto_log"], inplace=True)
                elif s[:14] == "avg_daily_bill":
                    # ATV: get lower and upper bounds
                    df["atv_log"] = np.log(df[s])
                    atv_log_mean = df[df[s] > 0].atv_log.mean()
                    atv_log_std = df[df[s] > 0].atv_log.std(ddof=1)
                    atv_lower = round(np.exp(atv_log_mean - atv_log_std * 3))
                    atv_upper = round(rto_upper / 2.5)
                    atv_mask = (df[s] >= atv_lower) & (df[s] <= atv_upper)
                    logging.info(f"ATV bounds: ({atv_lower}, {atv_upper})")
                    mask = mask & atv_mask
                    df.drop(columns=["atv_log"], inplace=True)
                elif s[:8] == "trn_days":
                    # CNT_VIS: get lower and upper bounds
                    cnt_trn_q_25 = df[s].quantile(0.25)
                    cnt_trn_q_75 = df[s].quantile(0.75)
                    cnt_trn_iqr = cnt_trn_q_75 - cnt_trn_q_25
                    cnt_trn_lower = round(cnt_trn_q_25 - 1.5 * cnt_trn_iqr)
                    cnt_trn_upper = round(cnt_trn_q_75 + 1.5 * cnt_trn_iqr)
                    cnt_mask = (df[s] >= cnt_trn_lower) & (
                        df[s] <= cnt_trn_upper
                    )
                    logging.info(
                        f"N_TRN bounds: ({cnt_trn_lower}, {cnt_trn_upper})"
                    )
                    mask = mask & cnt_mask
                else:
                    logging.info("Unknown statistics")
        else:
            logging.info("Unknown def_outliers_by")
            logging.info("Cleaning from outliers failed")
            logging.info("Initial dataframe returned")
            data_f["cat_" + list_to_str(cat_name).lower()] = df
        # Filtering Customers by RTO & ATV
        # mask = rto_mask & atv_mask & cnt_mask
        try:
            df = df[mask]
        except:
            logging.info("Cleaning from outliers failed")
            logging.info("Initial dataframe returned")
            data_f["cat_" + list_to_str(cat_name).lower()] = df

        n_cust_final = df.shape[0]

        # Filtering Data Predict
        n_cust_drop_by_enrol_date = n_cust - n_cust_after_filt_enrolment
        n_cust_drop_by_sigma_rule = n_cust_after_filt_enrolment - n_cust_final

        logging.info(f"N customers before filtering: {n_cust}")
        logging.info(
            f"N customers filtered by enrolment date: {n_cust_drop_by_enrol_date}"
        )
        logging.info(
            f"N customers filtered by {[s for s in statistics if s!='enrollment_days']}: {n_cust_drop_by_sigma_rule}"
        )
        logging.info(f"N customers final for scoring: {n_cust_final}")
        logging.info("Data cleaned from outliers!")
        data_f["cat_" + list_to_str(cat_name).lower()] = df
    return data_f


def score_model(parameters: Dict, clfs: Dict, calib_clfs: Dict, data: Dict) -> Tuple:
    logging.basicConfig(
        filename="data/08_reporting/execution_training.log",
        filemode="w",
        format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        level=logging.DEBUG,
    )
    parameters1 = parameters.copy()
    predictions = {}
    if parameters["brand_name"]:
        for cat_name, brand_name in zip(
            parameters["cat_name"], parameters["brand_name"]
        ):
            logging.info(
                "Starting scoring for category: {0} brand: {1}".format(
                    cat_name, brand_name
                )
            )
            logging.info("Importing data for scoring")
            parameters1["cat_name"] = cat_name
            parameters1["brand_name"] = brand_name
            X = data["cat_" + list_to_str(cat_name).lower()]
            clf = clfs[
                "cat_"
                + list_to_str(cat_name).lower()
                + "_"
                + "brand_"
                + list_to_str(brand_name).lower()
            ]
            calib_clf = calib_clfs["cat_"
                + list_to_str(cat_name).lower()
                + "_"
                + "brand_"
                + list_to_str(brand_name).lower()]
            logging.info("Scoring data shape: {}".format(X.shape))
            logging.info("Starting scoring")
            probs = clf.predict_proba(X)
            preds = probs[:, 1]
            calib_preds = calib_clf.predict_proba(X)[:,1]
            X_score = X[[]]
            X_score["score"] = preds
            X_score["calibrated_score"] = calib_preds
            X_score = X_score.reset_index()
            # X_score["cross_val_score"] = preds
            X_score["score_type"] = "predicted_score"
            X_score["calc_date"] = parameters["calc_dt"]
            X_score["description"] = parameters["description"]
            X_score["cat_name"] = list_to_str(parameters1["cat_name"])
            X_score["brand_name"] = list_to_str(parameters1["brand_name"])
            X_score.columns = [
                "contact_id",
                "score",
                "calibrated_score",
                "score_type",
                "calc_date",
                "description",
                "cat_name",
                "brand_name",
            ]
            X_score = X_score[
                [
                    "contact_id",
                    "score",
                    "calibrated_score",
                    "score_type",
                    "calc_date",
                    "description",
                    "cat_name",
                    "brand_name",
                ]
            ]
            output_table = reformat_type(X_score)
            # logging.info("Starting to export to database")
            # su.save_to_gp(output_table, parameters["output_table_name"], chunksize=10_000)
            # logging.info("output_table saved in %s", parameters["output_table_name"])
            predictions[
                "cat_"
                + list_to_str(cat_name).lower()
                + "_"
                + "brand_"
                + list_to_str(brand_name).lower()
            ] = output_table[["contact_id", "score","calibrated_score"]].set_index("contact_id")
    else:
        for cat_name in parameters["cat_name"]:
            logging.info("Starting scoring for category: {0}".format(cat_name))
            logging.info("Importing data for scoring")
            parameters1["cat_name"] = cat_name
            X = data["cat_" + list_to_str(cat_name).lower()]
            clf = clfs["cat_" + list_to_str(cat_name).lower()]
            calib_clf = calib_clfs["cat_" + list_to_str(cat_name).lower()]
            logging.info("Scoring data shape: {}".format(X.shape))
            logging.info("Starting scoring")
            probs = clf.predict_proba(X)
            preds = probs[:, 1]
            calib_preds = calib_clf.predict_proba(X)[:,1]
            X_score = X[[]]
            X_score["score"] = preds
            X_score["calibrated_score"] = calib_preds
            X_score = X_score.reset_index()
            # X_score["cross_val_score"] = preds
            X_score["score_type"] = "predicted_score"
            X_score["calc_date"] = parameters["calc_dt"]
            X_score["description"] = parameters["description"]
            X_score["cat_name"] = list_to_str(parameters1["cat_name"])
            X_score["brand_name"] = "False"
            X_score.columns = [
                "contact_id",
                "score",
                "calibrated_score",
                "score_type",
                "calc_date",
                "description",
                "cat_name",
                "brand_name",
            ]
            X_score = X_score[
                [
                    "contact_id",
                    "score",
                    "calibrated_score",
                    "score_type",
                    "calc_date",
                    "description",
                    "cat_name",
                    "brand_name",
                ]
            ]
            output_table = reformat_type(X_score)
            # logging.info("Starting to export to database")
            # su.save_to_gp(
            #     output_table, parameters["output_table_name"]
            # )
            # logging.info("output_table saved in %s", parameters["output_table_name"])
            predictions["cat_" + list_to_str(cat_name).lower()] = output_table[
                ["contact_id", "score", "calibrated_score"]
            ].set_index("contact_id")
    return [predictions, output_table]


def save_to_db_score(parameters: Dict, output_table: pd.DataFrame) -> None:
    su.drop_table_gp(parameters["output_table_name"])
    output_table = output_table[
        ["contact_id", "score","calibrated_score", "cat_name", "brand_name"]
    ]
    #output_table = output_table.reset_index(drop=True)
    logging.info("Starting to export to database")
    su.save_to_gp(output_table, parameters["output_table_name"])
    logging.info("output_table saved in %s", parameters["output_table_name"])
