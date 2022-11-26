"""
This module defines the following routines used by the 'split' step:

- ``create_dataset_filter``: Defines customizable logic for filtering the training, validation,
  and test datasets produced by the data splitting procedure. Note that arbitrary transformations
  should go into the transform step.
  
"""

from pandas import DataFrame, Series

"""
FIXES!

(1) Need to start by reformatting the input to match the template here: https://github.com/mlflow/recipes-classification-template/blob/main/steps/split.py
"""

def create_dataset_filter(train_df: DataFrame, validation_df: DataFrame, test_df: DataFrame) -> Series(bool):
    """
    Mark rows of the split datasets to be additionally filtered. This function will be called on
    the training, validation, and test datasets.
    
    :param dataset: The {train,validation,test} dataset produced by the data splitting procedure.
    :return: A Series indicating whether each row should be filtered
    
    """
    # FIXME::OPTIONAL: implement post-split filtering on the dataframes, such as data cleaning.
    
    dfn = train_df.loc[train_df.isFraud == 0, :]
    dfy = train_df.loc[train_df.isFraud == 1, :]
    
    N = len(train_df)
    y = len(dfy)
    p = y/N    # p = fraction of transactions that are fraudulent
    train_bal_df = dfn.sample(frac=p).union(dfy)

    #return Series(True, index=dataset.index)
    return train_bal_df, validation_df, test_df