# Databricks notebook source
import unittest
import pandas
import os, unittest
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
#from data_loader import get_data_and_labels

spark = SparkSession.builder \
    .appName('integrity-tests') \
    .getOrCreate()


# Does the specified table exist in the specified database?

def tableExists(tableName, dbName):
    return spark.catalog._jcatalog.tableExists(tableName)   #(f"{dbName}.{tableName}")


# Does the specified column exist in the given DataFrame?
def columnExists(dataFrame, columnName):
    if columnName in dataFrame.columns:
        return True
    else:
        return False


# How many rows are there for the specified value in the specified column
# in the given DataFrame?
def numRowsInColumnForValue(dataFrame, columnName, columnValue):
    df = dataFrame.filter(col(columnName) == columnValue)
    return df.count()


def should_load_data_and_return_features_labels(self):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    print(current_dir)
    # test_data_path = '{}/fixtures/review_10_samples.json'.format(current_dir)
    # with open(test_data_path) as file:
    # expected_number_of_examples = sum(1 for line in file)

    # texts, labels, _ = get_data_and_labels(test_data_path)

    # self.assertEqual(len(texts), expected_number_of_examples)
    # self.assertEqual(len(labels), expected_number_of_examples)

    # [self.assertTrue(label == 0 or label == 1) for label in labels]
    # [self.assertTrue(isinstance(text, str)) for text in texts]
