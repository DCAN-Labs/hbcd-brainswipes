import pandas as pd
import argparse
import numpy as np
import json

def main():
    parser = argparse.ArgumentParser(description='Convert the BrainSwipes CSV results file to a TSV file following HBCD specs.')
    parser.add_argument('input_csv', type=str, help='Path to the input CSV file')
    args = parser.parse_args()

    df = pd.read_csv(args.input_csv)

    with open('modalities.json', 'r') as file:
        mods = json.load(file)
    
    dmri = mods['dmri']

    # Create dmri DataFrame
    df_dmri = pd.DataFrame()

    ## Patient ID, Sub ID, Run
    df_dmri['participant_id'] = df['sample'].str.split('_').str[0]
    df_dmri['session_id'] = df['sample'].str.split('_').str[1]
    df_dmri['run_id'] = df['sample'].str.split('_').str[2] ## NEED TO DOUBLE CHECK THIS IS CORRECT FOR HBCD DWI

    ## Temporary columns
    df_dmri['temp_mean'] = df['aveVote']
    df_dmri['temp_nrev'] = df['count']
    df_dmri['temp_mod'] = df['sample'].str.split('_').str[4] ## NEED TO DOUBLE CHECK THIS IS CORRECT FOR HBCD DWI

    # Filter for each modality type and merge to output df
    df_merge = pd.DataFrame(columns=["participant_id", "session_id", "run_id"])
    for mod in dmri:
        df_filt = df_dmri[df_dmri['temp_mod'].str.contains(mod)]
        df_filt = df_filt.drop('temp_mod', axis=1)
        df_filt.rename(columns={"temp_mean": f"{mod}_mean"}, inplace=True)
        df_filt.rename(columns={"temp_nrev": f"{mod}_nrev"}, inplace=True)

        # Insert new QC column for cleaner column ordering
        df_filt[f"{mod}_QC"]=np.nan

        df_merge=pd.merge(df_merge, df_filt, on=["participant_id", "session_id", "run_id"], how="outer")

    # Adjust values after merging
    for index, row in df_merge.iterrows():
        for mod in dmri:
        # Set nrev to 0 if NaN 
            if pd.isna(row[f"{mod}_nrev"]):
                df_merge.at[index, f"{mod}_nrev"] = 0

        # Determine QC column values
            if row[f"{mod}_nrev"] < 10:
                df_merge[f"{mod}_QC"] = np.nan # Incomplete
            elif row[f"{mod}_mean"] < 0.7:
                df_merge[f"{mod}_QC"] = 0 # Fail
            elif row[f"{mod}_mean"] >= 0.7:
                 df_merge[f"{mod}_QC"] = 1 # Pass

    # Fill values for DWI_QC - take sum of all QC columns - should be equal to 7 if passing and assigned value of 1. otherwise will be assigned 0 (Fail) or NaN if NaNs were present in columns sum was derived from 
    qc_columns = [col for col in df_merge.columns if "QC" in col]
    df_merge['QC_Sum'] = df_merge[qc_columns].sum(axis=1, skipna=False) # include NaNs
    df_merge['DWI_QC'] = np.where(df_merge['QC_Sum'].isna(), np.nan, (df_merge['QC_Sum'] == 7).astype(int))

    # Drop sum columns and save 
    df_merge = df_merge.drop('QC_Sum', axis=1)
    df_merge.to_csv('img_brainswipes_qsiprep_dwi.tsv', index=None, na_rep='NA', sep='\t')
            
if __name__ == "__main__":
    main()