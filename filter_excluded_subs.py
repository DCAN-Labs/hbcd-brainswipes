import pandas as pd

# Load the data
included_df = pd.read_csv('files/participants_included.tsv', sep='\t')
data_df = pd.read_csv('files/img_brainswipes_xcpd-T2w.tsv', sep='\t')

# Filter the data_df to keep only rows with participant_ids in included_df
filtered_df = data_df[data_df['participant_id'].isin(included_df['participant_id'])]

excluded_df = data_df[~data_df['participant_id'].isin(included_df['participant_id'])]

# Optionally save the filtered result
filtered_df.to_csv('filtered_data.tsv', sep='\t', index=False)
excluded_df.to_csv('excluded_data.tsv', sep='\t', index=False)
