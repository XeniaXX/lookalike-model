"""Project pipelines."""
from typing import Dict

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline
from lookalike_for_brands.pipelines import score_pipeline as score_pipeline
from lookalike_for_brands.pipelines import train_pipeline as train_pipeline


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    pipelines = find_pipelines()
    train = train_pipeline.create_pipeline()
    score = score_pipeline.create_pipeline()
    return {"__default__": train + score, "train": train, "score": score}

