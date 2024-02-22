import pandas as pd
import numpy as np
#used pandas only to load dataframe
df = pd.read_csv('police_fatality_with_header.csv')
age_intervals = [(0, 18), (18, 30), (30, 50), (50, 70), (70, np.inf)]
age_labels = ['0-18', '18-30', '30-50', '50-70', '70+']

df['Age Group'] = pd.cut(df['Age'], bins=[0, 18, 30, 50, 70, np.inf], labels=age_labels, right=False)

percentage_by_state_and_age = df.groupby(['City', 'Age Group']).size().groupby(level=0).apply(
    lambda x: 100 * x / float(x.sum())
).unstack().fillna(0)

#save output as csv file
percentage_by_state_and_age.to_csv('percentage_by_state_and_age.csv')
