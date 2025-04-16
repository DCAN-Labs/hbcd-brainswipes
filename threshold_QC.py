

import pandas as pd

def read_and_filter_tsv(file_path, threshold):
    """
    Reads an HBCD BrainSwipes TSV file into a DataFrame and keeps subject rows where the overall average QC value is greater than or equal to the specified threshold.

    Parameters:
    - file_path (str): Path to the .tsv file
    - threshold (float): Threshold value for filtering

    Returns:
    - pd.DataFrame: Filtered DataFrame
    """
    df = pd.read_csv(file_path, sep='\t')

    fourth_col = df.columns[3]
    return df[df[fourth_col] >= threshold]

filtered_df = read_and_filter_tsv("files/img_brainswipes_xcpd_T2w.tsv", 0.6)
print(filtered_df.head())

