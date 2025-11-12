import pandas as pd
import argparse
import numpy as np
import json

def main():
    parser = argparse.ArgumentParser(description='Convert the BrainSwipes CSV results file to a TSV file following HBCD specs.')
    parser.add_argument('input_csv', type=str, help='Path to the input CSV file')
    args = parser.parse_args()

    df = pd.read_csv(args.input_csv)

    # Check that hash is same for all rows and then grab string to insert into fieldnames 
    df_hash = pd.DataFrame()
    df_hash['hash'] = df['sample'].str.split('_').str[2]
    hash = df_hash['hash'].unique().tolist()
    if len(hash) !=1:
        print("Error - multiple hashes")
    else:
        hash_n=hash[0]
        print(hash_n)

    # Filter out anat rows
    df = df[df['sample'].str.contains('bold')]

    tx = "T2w" # for MCRIBS surf recon
    df_fmri = pd.DataFrame()

    ## Patient ID, Sub ID, Run
    df_fmri['participant_id'] = df['sample'].str.split('_').str[0]
    df_fmri['session_id'] = df['sample'].str.split('_').str[1]
    df_fmri['run_id'] = df['sample'].str.split('_').str[5]

    ## Temporary columns
    df_fmri['temp_mean'] = df['aveVote']
    df_fmri['temp_nrev'] = df['count']
    df_fmri['temp_mod'] = df['sample'].str.split('_').str[7]

    # 0: sub-1006117941_
    # 1: ses-V02_
    # 2: hash-0f306a2f+0ef9c88a_
    # 3: task-rest_
    # 4: dir-PA_
    # 5: run-1_
    # 6: space-MNI152NLin6Asym_
    # 7: desc-T1wOnTaskBrainSwipes_
    # 8: bold

    # Filter out anat rows
    df = df[df['sample'].str.contains('bold')]

    # Filter for each modality type and merge to output df
    df_merge = pd.DataFrame(columns=["participant_id", "session_id", "run_id"])
    fmri = [f"{tx}OnTask", f"TaskOn{tx}"]

    for mod in fmri:
        df_filt = df_fmri[df_fmri['temp_mod'].str.contains(mod)]
        df_filt = df_filt.drop('temp_mod', axis=1)
        df_filt.rename(columns={"temp_mean": f"{mod}_mean", "temp_nrev": f"{mod}_nrev"}, inplace=True)

        df_merge = pd.merge(df_merge, df_filt, on=["participant_id", "session_id", "run_id"], how="outer")

    # Adjust values after merging - set nrev to 0 if NaN 
    for mod in fmri:
        df_merge[f"{mod}_nrev"] = df_merge[f"{mod}_nrev"].fillna(0)

    # Create summary_QC and summary_nrev columns
    mean_columns = [col for col in df_merge.columns if "_mean" in col]
    nrev_columns = [col for col in df_merge.columns if "_nrev" in col]
    df_merge['summary_QC'] = df_merge[mean_columns].mean(axis=1)
    df_merge['summary_nrev'] = df_merge[nrev_columns].mean(axis=1)
    
    # Reorder columns
    columns_list = ["participant_id", "session_id", "run_id", 'summary_QC', 'summary_nrev']
    for mod in fmri:
        columns_list.extend([f"{mod}_mean", f"{mod}_nrev"])
    df_merge = df_merge.reindex(columns=columns_list)

    df_merge = df_merge.round(decimals=3)

    # Ensure that all *_nrev columns are set to integer except *_summary_nrev
    cols_to_int = [
        c for c in df_merge.columns
        if c.endswith('_nrev') and not c.endswith('summary_nrev')
        ]

    df_merge[cols_to_int] = df_merge[cols_to_int].astype('int') 

    # Prepend table name to all column headers except first 3
    new_columns = df_merge.columns[:3].tolist() + [f'img_brainswipes_xcpd_{hash_n}_bold_' + col for col in df_merge.columns[3:]]
    df_merge.columns = new_columns

    # Save combined TSV
    df_merge.to_csv(f'img_brainswipes_xcpd_{hash_n}_bold.tsv', index=None, na_rep='NA', sep='\t')

if __name__ == "__main__":
    main()
