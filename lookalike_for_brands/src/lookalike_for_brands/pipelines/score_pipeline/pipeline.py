"""
This is a boilerplate pipeline 'score_pipeline'
generated using Kedro 0.18.7
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import collect_score_data, score_model, remove_outliers, save_to_db_score


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=collect_score_data,
                inputs=["params:data_options_score"],
                outputs=["score_data"],
                name="collect_score_data",
            ),
            node(
                func=remove_outliers,
                inputs=["score_data", "params:data_options_score"],
                outputs="score_data_filtered",
                name="remove_outliers",
            ),
            node(
                func=score_model,
                inputs=[
                    "params:data_options_score",
                    "clfs",
                    "calib_clfs",
                    "score_data_filtered",
                ],
                outputs=["predictions", "output_table"],
                name="score_model",
            ),
            node(
                func=save_to_db_score,
                inputs=[
                    "params:data_options_score",
                    "output_table",
                ],
                outputs=None,
                name="save_to_db_score",
            ),
        ]
    )
