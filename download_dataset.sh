#!/bin/bash
curl -L -o sales-dataset.zip\
  https://www.kaggle.com/api/v1/datasets/download/shantanugarg274/sales-dataset
  unzip sales-dataset.zip
  rm sales-dataset.zip
