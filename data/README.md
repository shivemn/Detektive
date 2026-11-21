# Data

Nothing in this folder is committed. Download the data yourself.

## IBM Transactions for Anti-Money Laundering (AMLworld)

Source: https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml
Licence: Community Data License Agreement – Sharing 1.0

The data is **synthetic**. It comes from IBM's agent-based simulator, not from a
real bank. Every claim in this repo about accuracy is a claim about this
simulator unless it says otherwise.

Put the files here:

```
data/raw/HI-Small_Trans.csv      # 5,078,345 transactions, start here
data/raw/HI-Small_Patterns.txt   # the laundering attempts, by pattern type
```

Then convert the CSV to Parquet once (much faster to load afterwards):

```
python -m detektive_ml.prepare data/raw/HI-Small_Trans.csv data/processed/hi_small.parquet
```

The other sizes (LI-Small, HI-Medium, ...) follow the same file naming and
load the same way.
