"""
Copyright 2022 pgdunn@bu.edu (pdvnny)
Created Nov 24, 2022

This module defines the following routines used by the "transform" step:

- `transformer_fn`: Defines the steps for transforming input data before it is passed to the estimator
  during model inference
  
"""

"""
The pyspark.ml approach used originally does not work in this context (the MLflow recipes context)

    
    # Steps of "Pipeline()"
    # (1) StringIndexer
    # (2) VectorAssembler
    

    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StringIndexer
    from pyspark.ml.feature import VectorAssembler
    
    # Encodes a string column of labels to a column of label indices
    indexer = StringIndexer(inputCol="type", outputCol="typeIndexed")
    
    # VectorAssembler is a transformer that combines a given list of columns into a single vector column
    vec_assembler = VectorAssembler(inputCols=["typeIndexed", "amount",
                                               "oldBalanceOrg", "newbalanceOrig",
                                               "oldBalanceDest", "newbalanceDest"],
                                   outputCol="features"
                                   )
    
    transformer = Pipeline(stages=[indexer, vec_assembler])
    
    return transformer
"""

"""
    NOTE
    
    I moved the creation of a column called "typeIndexed" to the ingestion step!!

"""

def transformer_fn():
    """
    Returns an *unfitted* transformer that defines ``fit()`` and ``transform()`` methods.
    The transformer's input and output signatures should be compatible with scikit-learn
    transformers.
    """
    
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import LabelEncoder
    
    
    # Reference for this class below: https://towardsdatascience.com/creating-custom-transformers-for-sklearn-pipelines-d3d51852ecc1
    from sklearn.base import BaseEstimator, TransformerMixin
    
    # The ColumnsSelector class inherits from the sklearn.base classes 
    # (BaseEstimator, TransformerMixin). This makes it compatible with 
    # scikit-learn’s Pipelines
    class ColumnsSelector(BaseEstimator, TransformerMixin):
        # initializer 
        def __init__(self, columns):
            # save the features list internally in the class
            self.columns = columns

        def fit(self, X, y = None):
            return self
        def transform(self, X, y = None):
            # return the dataframe with the specified features
            return X.loc[:, self.columns]
    
    
    transformer = Pipeline(steps=[
        ('columns selector', ColumnsSelector(["typeIndexed", "amount",
                                               "oldbalanceOrg", "newbalanceOrig",
                                               "oldbalanceDest", "newbalanceDest"])),
    ])
    
    return transformer