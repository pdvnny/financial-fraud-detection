"""
Copyright 2022 pgdunn@bu.edu (pdvnny)
Created Nov 24, 2022

This module defines the following routines used by the "transform" step:

- `transformer_fn`: Defines the steps for transforming input data before it is passed to the estimator
  during model inference
  
"""

def transformer_fn():
    """
    Returns an *unfitted* transformer that defines ``fit()`` and ``transform()`` methods.
    The transformer's input and output signatures should be compatible with scikit-learn
    transformers.
    """
    
    """
    Steps of "Pipeline()"
    (1) StringIndexer
    (2) VectorAssembler
    """
    
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StringIndexer
    from pyspark.ml.feature import VectorAssembler
    
    # Encodes a string column of labels to a column of label indices
    indexer = StringIndexer(inputCol="type", outputCol="typeIndexed")
    
    # VectorAssembler is a transformer that combines a given list of columns into a single vector column
    vec_assembler = VectorAssembler(inputsCols=["typeIndexed", "amount",
                                               "oldBalanceOrg", "newbalanceOrig",
                                               "oldBalanceDest", "newbalanceDest"],
                                   outputCol="features"
                                   )
    
    transformer = Pipeline(stages=[indexer, vec_assembler])
    
    return transformer