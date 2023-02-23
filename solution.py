import pandas as pd


# Import data from file to dataframe and label columns
df = pd.read_csv('./sample.log', sep=" ", header=None,
                names=["datestamp", "GUID", "action", 
                       "url", "None", "status", "None1", "type"])

# Drop emty colums: None and None1
df = df.drop(columns=['None', 'None1'], axis=1)

# Sort all data by GUID and timestam
df = df.sort_values(['GUID', 'datestamp'], ascending=[True, True])

# Transform datestamp from string to datetime type
df['datestamp'] = pd.to_datetime(df['datestamp'])

# Compute the difference between front -> back & back -> front
df['difference'] = df['datestamp'].diff()

# Fill NaT values for first frontend entry 
df.loc[df['url'] != "-", 'difference'] = 'NaT'

# Fill culprit column
df['culprit'] = df['type'].shift(1)
df.loc[df['url'] != "-", 'culprit'] = 'NaT'

# Sort descending by time response 
df = df.sort_values(['difference'], ascending=[False])

# Drop duplicate entries
df = df.drop_duplicates(subset=['culprit'])
df = df[:-1]

# Print slow servers
print(df['culprit'].to_string(index=False))