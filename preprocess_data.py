import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
import itertools

def read_dataset(csv_filename):
    data_original = pd.read_csv(csv_filename)
    data_original['date'] = pd.to_datetime(data_original['date'])
    # Rounding off the total daily KW values to 3 decimal places
    data_original['total daily KW'] = data_original['total daily KW'].apply(lambda x: round(x, 3))
    return data_original

def remove_sporadic_ids(threshold, count_of_values):
    d = count_of_values.to_dict()
    removable = []
    for value in d.items(): # looping through every item in the dictionary 
        if value[1] < threshold:
            removable.append(value[0])
    return removable

def rearrange_dataset(removable_ids, data_original):
    total_uniq_id = 7445
    total_time_limit = 536
    data = pd.DataFrame({'date':pd.date_range('2009-07-14', periods = total_time_limit, freq='D',)})
    for i in range(1000, total_uniq_id):
        if i not in removable_ids:
            S = data_original[data_original['Meter ID'] == i][['date','total daily KW']]
            data = pd.merge(data, S, how ='left', on='date')

    r, c = data.shape
    a = 1
    for x in range(1000, total_uniq_id):
        if x not in removable_ids and a <= (c - 1):
            data.columns.values[a] = str(x)
            a += 1
    return data

def interpolate_data(data):
    interpolated_data = data.interpolate(method='linear', axis=0).ffill().bfill()
    interpolated_data = interpolated_data.dropna(axis='columns',how='any')
    return interpolated_data

print 'Reading dataset...'
data_original = read_dataset('daily_electricity_usage.csv')
print 'Removing sporadic items...'
removable_ids = remove_sporadic_ids(100, data_original.groupby('Meter ID')['total daily KW'].count())
print 'Rearranging dataset...'
data = rearrange_dataset(removable_ids, data_original)
print 'Interpolating data...'
interpolated_data = interpolate_data(data)
print 'Expanding dataset...'
# Data Set Expansion
list_hours = []
for i in range(0, 24):
    list_hours.append(' ' + "{0:0=2d}".format(i) + ':00:00')
list_hours_daily = []
for i in range(0, 536):
    list_hours_daily.append(list_hours)
list_hours_daily_flat = list(itertools.chain(*list_hours_daily))
expanded_data = pd.DataFrame({'date': np.repeat((interpolated_data['date']), 24), 'time': list_hours_daily_flat})
print 'Got list of hours...'
expanded_data['date'] = expanded_data['date'].dt.strftime('%Y-%m-%d') + expanded_data['time']
del expanded_data['time']
expanded_data['date'] = pd.to_datetime(expanded_data['date'], format = "%Y-%m-%d %H:%M:%S")
for i in interpolated_data.columns:
    if i != 'date':
        expanded_data[str(i)] = np.repeat(interpolated_data[str(i)] / 24, 24)

print 'Writing expanded dataset to csv...'
# expanded_data[:100].to_csv(r'expanded_data_cleaned_reduced.csv', index = False)
expanded_data.to_csv(r'expanded_data_cleaned.csv', index = False)

# Extracting Features
interpolated_data.date = pd.to_datetime(interpolated_data.date)
interpolated_data['day'] = interpolated_data['date'].apply(lambda x:x.weekday()) # populating days in days column
interpolated_data['months'] = interpolated_data['date'].dt.to_period('M') # assigning months values to months column
interpolated_data['quarter'] = interpolated_data['date'].dt.quarter # populating quarter in quarter column
interpolated_data['quarter'] = pd.cut(interpolated_data['quarter'], bins=[0, 1, 2, 3, 4], labels=['First', 'Second', 'Third', 'Fourth'])
uniq_meter_ids = interpolated_data.columns[1:-3] #Just selec 

uniq_meter_ids = interpolated_data.columns[1:-3] #Just selec 
data_featured = pd.DataFrame({'Meter ID':uniq_meter_ids.values, 'total KW':np.sum(interpolated_data[uniq_meter_ids]).values})
data_featured['average per day']= interpolated_data[uniq_meter_ids].mean().values
data_featured['% Monday']= interpolated_data[interpolated_data['day'] == 0][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% Tuesday']= interpolated_data[interpolated_data['day'] == 1][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% Wednesday']= interpolated_data[interpolated_data['day'] == 2][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% Thursday']= interpolated_data[interpolated_data['day'] == 3][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% Friday']= interpolated_data[interpolated_data['day'] == 4][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% Saturday']= interpolated_data[interpolated_data['day'] == 5][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% Sunday']= interpolated_data[interpolated_data['day'] == 6][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% weekday']= interpolated_data[(interpolated_data['day'] != 5) & (interpolated_data['day'] != 6)][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% weekend']= interpolated_data[(interpolated_data['day'] == 5) | (interpolated_data['day'] == 6)][uniq_meter_ids].sum().values / data_featured['total KW'] * 100

# For Months
data_featured['% January']= interpolated_data[interpolated_data['months'].dt.month == 1][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% Feb']= interpolated_data[interpolated_data['months'].dt.month == 2][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% March']= interpolated_data[interpolated_data['months'].dt.month == 3][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% April']= interpolated_data[interpolated_data['months'].dt.month == 4][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% May']= interpolated_data[interpolated_data['months'].dt.month == 5][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% June']= interpolated_data[interpolated_data['months'].dt.month == 6][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% July']= interpolated_data[interpolated_data['months'].dt.month == 7][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% August']= interpolated_data[interpolated_data['months'].dt.month == 8][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% September']= interpolated_data[interpolated_data['months'].dt.month == 9][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% October']= interpolated_data[interpolated_data['months'].dt.month == 10][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% November']= interpolated_data[interpolated_data['months'].dt.month == 11][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['% December']= interpolated_data[interpolated_data['months'].dt.month == 12][uniq_meter_ids].sum().values / data_featured['total KW'] * 100

# For Quarters
data_featured['%Q1']= interpolated_data[(interpolated_data['quarter'] == 'First')][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['%Q2']= interpolated_data[(interpolated_data['quarter'] == 'Second')][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['%Q3']= interpolated_data[(interpolated_data['quarter'] == 'Third')][uniq_meter_ids].sum().values / data_featured['total KW'] * 100
data_featured['%Q4']= interpolated_data[(interpolated_data['quarter'] == 'Fourth')][uniq_meter_ids].sum().values / data_featured['total KW'] * 100

data_featured=data_featured.fillna(0)

df_columns = data_featured.columns[1:]
scaller = StandardScaler()
matrix = pd.DataFrame(scaller.fit_transform(data_featured[df_columns]), columns = df_columns)
matrix['Meter ID'] = data_featured['Meter ID']
matrix.to_csv(r'extracted_features_scaled.csv', index = False)
