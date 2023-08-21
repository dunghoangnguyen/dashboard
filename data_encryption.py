# Databricks notebook source
# MAGIC %md
# MAGIC # THIS FUNCTION TAKES A DATASET AND ENCRYPT ITS SPECIFIED COLUMN(S)
# MAGIC ### Syntax: convert_column(dataset, column, rule)
# MAGIC <strong>dataset: Pandas dataset</strong><br>
# MAGIC <strong>column: from 1 ->, if empty means entire dataset will be converted</strong><br>
# MAGIC <strong>rule: 0 means generic, 1 means specific</strong>

# COMMAND ----------

import pandas as pd
from hashlib import sha256

def convert_column(dataset_name: pd.DataFrame, column_index: int = None, rule_number: int = 0):
    if column_index is not None:
        column_index -= 1
        if rule_number == 0:
            dataset_name.iloc[:, column_index] = dataset_name.iloc[:, column_index].apply(lambda x: sha256(str(x).encode()).hexdigest())
        elif rule_number == 1:
            # Apply specific rule here
            pass
    else:
        if rule_number == 0:
            for col in dataset_name.columns:
                dataset_name[col] = dataset_name[col].apply(lambda x: sha256(str(x).encode()).hexdigest())
        elif rule_number == 1:
            # Apply specific rule here
            pass

# COMMAND ----------


