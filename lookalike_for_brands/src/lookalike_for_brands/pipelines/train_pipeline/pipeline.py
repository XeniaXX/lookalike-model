"""
This is a boilerplate pipeline 'train_pipeline'
generated using Kedro 0.18.7
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import collect_train_data, train_model


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=collect_train_data,
                inputs=["params:data_options_train"],
                outputs=["train_data", "target", "customers"],
                name="collect_train_data",
            ),
            node(
                func=train_model,
                inputs=[
                    "params:data_options_train",
                    "train_data",
                    "target",
                ],
                outputs=["clfs","calib_clfs","cross_val_scores"],
                name="train_model",
            ),
        ]
    )
