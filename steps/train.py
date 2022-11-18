"""
This module defines the `train` routine for our financial fraud detection model

- The returned object is a trained CrossValidator Spark model

"""

from typing import Dict, Any
from pyspark.sql import DataFrame

def estimator_fn(estimator_params: Dict[str, Any] = {}, train: DataFrame):
    """
    Returns a trained estimator with fit and predict methods.
    
    The estimator will be a PySpark `DecisionTreeClassifier`
    """
    from pyspark.ml.classification import DecisionTreeClassifier
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    
    evaluatorAUC = BinaryClassificationEvaluator(labelCol = 'label', rawPredcitionCol = 'prediction', metricName = 'areaUnderROC')

    dt = DecisionTreeClassifier(**estimator_params)  # TO DO: double check that this setup works
    
    paramGrid = (
        ParamGridBuilder()
            .addGrid(dt.maxDepth, [5, 10, 15])
            .addGrid(dt.maxBins, [10, 20, 30])
            .build()
    )
    
    cv = CrossValidator(
        estimator = dt,
        estimatorParamMaps = paramGrid,
        evaluator = evaluatorPR,
        numFolds = 3
    )
    
    model = cv.fit(train)
    
    return model