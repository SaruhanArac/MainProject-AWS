# # Pandas way of dropping name and card number columns
import pandas as pd
import csv

csv_file = 'leeds_09-05-2023_09-00-00.csv'


columns = ['Timestamp', 'Location', 'Customer Name', 'Items Ordered', 'Total Amount', 'Payment Method', 'Card Number']
df = pd.read_csv(csv_file, names=columns)


if 'Card Number' in df.columns:
    df.drop(columns=['Card Number'], inplace=True)
if 'Customer Name' in df.columns:
    df.drop(columns=['Customer Name'], inplace=True)


print(df)

print ('NEW TABLE............')
csv_file = 'chesterfield_25-08-2021_09-00-00.csv'


columns = ['Timestamp', 'Location', 'Customer Name', 'Items Ordered', 'Total Amount', 'Payment Method', 'Card Number']
df = pd.read_csv(csv_file, names=columns)


if 'Card Number' in df.columns:
    df.drop(columns=['Card Number'], inplace=True)
if 'Customer Name' in df.columns:
    df.drop(columns=['Customer Name'], inplace=True)


print(df)
