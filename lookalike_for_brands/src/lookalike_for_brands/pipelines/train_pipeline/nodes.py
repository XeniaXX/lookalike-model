"""
This is a boilerplate pipeline 'train_pipeline'
generated using Kedro 0.18.7
"""
import re
from typing import Dict, Tuple
import pandas as pd

from lookalike_for_brands.pipelines.functions import *
import magpie.sql_utils as su


def collect_train_data(parameters: Dict) -> Tuple:
    if parameters["brand_name"]:
        logging.info(
            "Collecting customer transactions to find customers for training (mode=brands)"
        )
        collect_customer_trn(parameters)
    else:
        logging.info(
            "Collecting customer transactions to find customers for training (mode=categories)"
        )
        collect_customer_trn1(parameters)
    parameters1 = parameters.copy()
    data = {}
    target = {}
    customers = {}
    if parameters["dist_flag"] == False:
        parameters["dist_name"] = [False for i in range(len(parameters["brand_name"]))]
    if parameters["brand_name"]:
        for cat_name, brand_name, dist_name in zip(
            parameters["cat_name"], parameters["brand_name"], parameters["dist_name"]
        ):
            # print("Starting collecting training data for category: ", cat_id, " brand: ", brand_id)
            logging.info(
                "Starting to collect training data for category: {0} brand: {1}".format(
                    cat_name, brand_name
                )
            )
            parameters1["cat_name"] = cat_name
            parameters1["brand_name"] = brand_name
            parameters1["dist_name"] = dist_name
            print("Parameters: ", parameters1)
            try:
                X = data["cat_" + list_to_str(cat_name).lower()]
                logging.info(
                    "Starting to collect target_df, no collection of features needed"
                )
                y, cust = collect_all_data(
                    train=True, collect_features=False, parameters=parameters1
                )
                logging.info("Finished collecting target_df")
                y = y.loc[X.index]
                target[
                    "cat_"
                    + list_to_str(cat_name).lower()
                    + "_"
                    + "brand_"
                    + list_to_str(brand_name).lower()
                ] = y
                customers[
                    "cat_"
                    + list_to_str(cat_name).lower()
                    + "_"
                    + "brand_"
                    + list_to_str(brand_name).lower()
                ] = cust
            except:
                logging.info("Starting to collect target_df and  features")
                X, y, cust = collect_all_data(
                    train=True, collect_features=True, parameters=parameters1
                )
                logging.info("Finished collecting target_df and features")
                logging.info("Collected data shape {}".format(X.shape))
                data["cat_" + list_to_str(cat_name).lower()] = X
                target[
                    "cat_"
                    + list_to_str(cat_name).lower()
                    + "_"
                    + "brand_"
                    + list_to_str(brand_name).lower()
                ] = y
                customers[
                    "cat_"
                    + list_to_str(cat_name).lower()
                    + "_"
                    + "brand_"
                    + list_to_str(brand_name).lower()
                ] = cust
        return [data, target, customers]
    else:
        for cat_name in parameters["cat_names"]:
            logging.info(
                "Starting to collect training data for category: {0}".format(cat_name)
            )
            # print("Starting collecting training data for category: ", cat_id)
            parameters1["cat_name"] = cat_name
            print("Parameters: ", parameters1)
            try:
                X = data["cat_" + list_to_str(cat_name).lower()]
                logging.info(
                    "Starting to collect target_df, no collection of features needed"
                )
                y, cust = collect_all_data(
                    train=True, collect_features=False, parameters=parameters1
                )
                logging.info("Finished collecting target_df")
                y = y.loc[X.index]
                target["cat_" + list_to_str(cat_name).lower()] = y
                customers["cat_" + list_to_str(cat_name).lower()] = cust
            except:
                logging.info("Starting to collect target_df and  features")
                X, y, cust = collect_all_data(
                    train=True, collect_features=True, parameters=parameters1
                )
                logging.info("Finished collecting target_df and features")
                logging.info("Collected data shape {}".format(X.shape))
                data["cat_" + list_to_str(cat_name).lower()] = X
                target["cat_" + list_to_str(cat_name).lower()] = y
                customers["cat_" + list_to_str(cat_name).lower()] = cust
        logging.info("Starting to delete tables from the database")
        clear_data_tables()
        logging.info("Deleted tables from the database")
        return [data, target, customers]


def train_model(parameters: Dict, data: Dict, target: Dict) -> Tuple:
    logging.basicConfig(
        filename="data/08_reporting/execution_training.log",
        filemode="w",
        format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        level=logging.DEBUG,
    )
    clfs = {}
    calib_clfs = {}
    parameters1 = parameters.copy()
    cross_val_scores = {}
    # create_output_table(parameters["output_table_name"])
    if parameters["brand_name"]:
        for cat_name, brand_name in zip(
            parameters["cat_name"], parameters["brand_name"]
        ):
            # print("Starting training for category: ", cat_id, " brand: ", brand_id)
            logging.info(
                "Starting training for category: {0} brand: {1}".format(
                    cat_name, brand_name
                )
            )
            parameters1["cat_name"] = cat_name
            parameters1["brand_name"] = brand_name
            # print("Parameters: ", parameters1)
            logging.info("Importing data for training")
            X, y = (
                data["cat_" + list_to_str(cat_name).lower()],
                target[
                    "cat_"
                    + list_to_str(cat_name).lower()
                    + "_"
                    + "brand_"
                    + list_to_str(brand_name).lower()
                ],
            )
            # print("Training data shape: ", X.shape)
            logging.info("Training data shape: {}".format(X.shape))
            clf, probs, calib_clf, calib_probs = train_eval_pipeline(
                LGBMClassifier(
                    n_estimators=200,  # 400
                    learning_rate=0.03,
                    num_leaves=30,
                    colsample_bytree=0.8,
                    subsample=0.9,
                    max_depth=7,
                    reg_alpha=0.1,
                    reg_lambda=0.1,
                    min_split_gain=0.01,
                    min_child_weight=2,
                    silent=-1,
                    verbose=-1,
                ),
                X,
                y,
            )
            preds = probs
            X_score = X[[]]
            X_score["score"] = preds
            X_score["calibrated_score"] = calib_probs
            X_score = X_score.reset_index()
            # X_score["cross_val_score"] = preds
            X_score["score_type"] = "cross_val_score"
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
            X_score = reformat_type(X_score)
            print(probs)
            print(X_score.head())
            # X_score = X_score[["contact_id", "score", "cat_name", "brand_name"]]
            # logging.info("Starting to export to database")
            # su.save_to_gp(X_score, parameters["output_table_name"], chunksize=10_000)
            clfs[
                "cat_"
                + list_to_str(cat_name).lower()
                + "_"
                + "brand_"
                + list_to_str(brand_name).lower()
            ] = clf
            calib_clfs[
                "cat_"
                + list_to_str(cat_name).lower()
                + "_"
                + "brand_"
                + list_to_str(brand_name).lower()
            ] = calib_clf
            cross_val_scores[
                "cat_"
                + list_to_str(cat_name).lower()
                + "_"
                + "brand_"
                + list_to_str(brand_name).lower()
            ] = X_score[["contact_id", "score", "calibrated_score"]].set_index("contact_id")
            # R_c = roc_auc(clf, X, y)
    else:
        for cat_name in parameters["cat_name"]:
            # print("Starting training for category: ", cat_id,)
            logging.info(
                "Starting tr get_cat_name(training for category: {0}".format(cat_name)
            )
            parameters1["cat_name"] = cat_name
            # print("Parameters: ", parameters1)
            logging.info("Importing data for training")
            X, y = (
                data["cat_" + list_to_str(cat_name).lower()],
                target["cat_" + list_to_str(cat_name).lower()],
            )
            print("Training data shape: ", X.shape)
            logging.info("Training data shape: {}".format(X.shape))
            clf, probs, calib_clf, calib_probs = train_eval_pipeline(
                LGBMClassifier(
                    n_estimators=400,
                    learning_rate=0.03,
                    num_leaves=30,
                    colsample_bytree=0.8,
                    subsample=0.9,
                    max_depth=7,
                    reg_alpha=0.1,
                    reg_lambda=0.1,
                    min_split_gain=0.01,
                    min_child_weight=2,
                    silent=-1,
                    verbose=-1,
                    n_jobs=16,
                ),
                X,
                y,
            )
            preds = probs
            X_score = X[[]]
            X_score["score"] = preds
            X_score["calibrated_score"] = calib_probs
            X_score = X_score.reset_index()
            # X_score["cross_val_score"] = preds
            X_score["score_type"] = "cross_val_score"
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
            X_score = reformat_type(X_score)
            # su.save_to_gp(X_score, parameters["output_table_name"], chunksize=10_000)
            clfs["cat_" + list_to_str(cat_name).lower()] = clf
            calib_clfs["cat_" + list_to_str(cat_name).lower()] = calib_clf
            cross_val_scores["cat_" + list_to_str(cat_name).lower()] = X_score[
                ["contact_id", "score", "calibrated_score"]
            ].set_index("contact_id")
            # R_c = roc_auc(clf, X, y)
    return [clfs, calib_clfs, cross_val_scores]
