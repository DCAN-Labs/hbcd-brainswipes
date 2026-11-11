import pandas as pd
from pathlib import Path

## HARDCODED INPUTS/VARIABLES
# Output directories
chat_dir = Path("data/parsed-chat")
output_dir = Path("data/BrainSwipes_updated_with_chat_results")

# Google Sheet ID where BrainSwipes chat info is pulled from
sheet_id = "1P6QFJaZjb-F5roWkzQXkoGFW1E95t9rge6RfNmmKozc"
px_gids = {
    "pt1": "898005270",
    "pt2": "1197223882"
}

# Hash IDs
hash_n = ['hash-d902942d+7a4c379b', 'hash-364caa63+7a4c379b']

# Modality types for structural MRI
smri = ["AnatOnAtlasBrainSwipes", "AtlasOnAnatBrainSwipes", "AxialBasalGangliaPutamen", "AxialSuperiorFrontal", "CoronalCaudateAmygdala", "CoronalOrbitoFrontal", "CoronalPosteriorParietalLingual", "SagittalInsulaFrontoTemporal", "SagittalInsulaTemporalHippocampalSulcus"]

## FUNCTIONS
def load_and_filter_parsed_chat(sheet_id, sheet_gid):

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={sheet_gid}"
    df = pd.read_csv(url, dtype=str, header=None).dropna(how="all")

    # Separate alternating user and assistant messages
    n = len(df) // 2
    df_chat = pd.DataFrame({
        "sample": df.iloc[:2*n:2, 0].to_numpy(),
        "Chat": df.iloc[1:2*n:2, 0].to_numpy()
    })

    # Filter flagged images associated with lmoore (exclude Pass)
    df_chat = df_chat[
        df_chat["Chat"].str.contains("lmoore", na=False) &
        ~df_chat["Chat"].str.contains("Pass", na=False)
    ]

    # Assign post-hoc ratings
    return df_chat[["sample"]].assign(aveVote=0, count=1)

def filter_and_merge(src_data, tx, hash):

    df = pd.read_csv(src_data)

    # Create anat and func dfs
    df_anat = df[~df['sample'].str.contains('bold')]
    df_func = df[df['sample'].str.contains('bold')]

    df_smri = pd.DataFrame()  # Create smri DataFrame
    df_fmri = pd.DataFrame()  # Create fmri DataFrame

    ## Patient ID, Sub ID, Run
    df_smri['participant_id'] = df_anat['sample'].str.split('_').str[0]
    df_smri['session_id'] = df_anat['sample'].str.split('_').str[1]
    df_smri['run_id'] = df_anat['sample'].str.split('_').str[3]

    df_fmri['participant_id'] = df_func['sample'].str.split('_').str[0]
    df_fmri['session_id'] = df_func['sample'].str.split('_').str[1]
    df_fmri['run_id'] = df_func['sample'].str.split('_').str[5]

    ## Temporary columns
    df_smri['temp_mean'] = df_anat['aveVote']
    df_smri['temp_nrev'] = df_anat['count']
    df_smri['temp_mod'] = df_anat['sample'].str.split('_').str[5] + '_' + df_anat['sample'].str.split('_').str[6]

    df_fmri['temp_mean'] = df_func['aveVote']
    df_fmri['temp_nrev'] = df_func['count']
    df_fmri['temp_mod'] = df_func['sample'].str.split('_').str[7]

    return df_smri, df_fmri

## WORK

# STEP 1: Parse Chat data from google sheet, save to CSV, and merge with BrainSwipes raw results to use for remaining workflow to generate TSV files for HBCD ingestion
for px, gid in px_gids.items():
    # Load and save filtered chat data
    df_chat = load_and_filter_parsed_chat(sheet_id, gid)
    df_chat.to_csv(chat_dir / f"assign-0-posthoc_{px}.csv", index=False)

    # Load BrainSwipes results
    df_BS_results = pd.read_csv(
        f"data/BrainSwipes_results_raw/BrainSwipes-HBCD3{px}-results.csv"
    )

    # Merge chat results with BrainSwipes results
    df_merge = pd.concat([df_chat, df_BS_results]).drop_duplicates(
        subset=df_BS_results.columns[0],
        keep="first"
    ).reset_index(drop=True)

    # Save updated results
    df_merge.to_csv(
        output_dir / f"BrainSwipes-HBCD3{px}-results_with_chat.csv",
        index=False
    )

# STEP 2: Generate TSV files for HBCD ingestion using the updated BrainSwipes results with parsed chat data
for hash in hash_n:
    if hash == 'hash-d902942d+7a4c379b':
        src_data = 'data/BrainSwipes_updated_with_chat_results/BrainSwipes-HBCD3pt1-results_with_chat.csv'
        tx='T2w'
    elif hash == 'hash-364caa63+7a4c379b':
        src_data = 'data/BrainSwipes_updated_with_chat_results/BrainSwipes-HBCD3pt2-results_with_chat.csv'
        tx='T1w'

    df_smri, df_fmri = filter_and_merge(src_data, tx, hash)

    # Filter for each modality type and merge to output df
    df_merge = pd.DataFrame(columns=["participant_id", "session_id", "run_id"])
    for mod in smri:
        df_filt = df_smri[df_smri['temp_mod'].str.contains(mod)]
        df_filt = df_filt.drop('temp_mod', axis=1)
        df_filt = df_filt.rename(columns={"temp_mean": f"{mod}_mean"})
        df_filt = df_filt.rename(columns={"temp_nrev": f"{mod}_nrev"})

        df_merge=pd.merge(df_merge, df_filt, on=["participant_id", "session_id", "run_id"], how="outer")

    # Adjust values after merging - set nrev to 0 if NaN 
    for index, row in df_merge.iterrows():
        for mod in smri:
            if pd.isna(row[f"{mod}_nrev"]):
                df_merge.at[index, f"{mod}_nrev"] = 0

    # Ensure that all *_nrev columns are set to integer (*_summary_nrev not yet created)
    cols_to_int = [
        c for c in df_merge.columns
        if c.endswith('_nrev')
        ]
    df_merge[cols_to_int] = df_merge[cols_to_int].astype(int)

    # Create summary_QC and summary_nrev columns from average across mean QC and mean nrev columns
    mean_columns = [col for col in df_merge.columns if "_mean" in col]
    nrev_columns = [col for col in df_merge.columns if "_nrev" in col]

    df_merge['summary_QC'] = df_merge[mean_columns].mean(axis=1)
    df_merge['summary_nrev'] = df_merge[nrev_columns].mean(axis=1)

    # Ensure column order is correctly set
    columns_list=["participant_id", "session_id", "run_id", "summary_QC", "summary_nrev"]
    for mod in smri:
        columns_list.append(f"{mod}_mean")
        columns_list.append(f"{mod}_nrev")
    df_merge = df_merge.reindex(columns=columns_list)

    # Drop 'Txw_' and 'BrainSwipes' from column headers as this will be redundant after prepending table name 
    df_merge.columns = df_merge.columns.str.replace(f'{tx}_', '')
    df_merge.columns = df_merge.columns.str.replace('BrainSwipes', '')

    # Prepend table name to all column headers except first 3
    new_columns = df_merge.columns[:3].tolist() + [f'img_brainswipes_xcpd_{hash}_{tx}_' + col for col in df_merge.columns[3:]]
    df_merge.columns = new_columns
    df_merge=df_merge.round(decimals=3)

    # Save HBCD TSV
    df_merge.to_csv(f'OUT/img_brainswipes_xcpd_{hash}_{tx}.tsv', index=False, na_rep='NA', sep='\t')

    # Filter for each modality type and merge to output df
    df_merge_fmri = pd.DataFrame(columns=["participant_id", "session_id", "run_id"])
    fmri = [f"{tx}OnTask", f"TaskOn{tx}"]

    for mod in fmri:
        df_filt = df_fmri[df_fmri['temp_mod'].str.contains(mod)]
        df_filt = df_filt.drop('temp_mod', axis=1)
        df_filt = df_filt.rename(columns={"temp_mean": f"{mod}_mean", "temp_nrev": f"{mod}_nrev"})

        df_merge_fmri = pd.merge(df_merge_fmri, df_filt, on=["participant_id", "session_id", "run_id"], how="outer")

    # Adjust values after merging - set nrev to 0 if NaN 
    for mod in fmri:
        df_merge_fmri[f"{mod}_nrev"] = df_merge_fmri[f"{mod}_nrev"].fillna(0)

    # Create summary_QC and summary_nrev columns
    mean_columns = [col for col in df_merge_fmri.columns if "_mean" in col]
    nrev_columns = [col for col in df_merge_fmri.columns if "_nrev" in col]
    df_merge_fmri['summary_QC'] = df_merge_fmri[mean_columns].mean(axis=1)
    df_merge_fmri['summary_nrev'] = df_merge_fmri[nrev_columns].mean(axis=1)

    # Reorder columns
    columns_list = ["participant_id", "session_id", "run_id", 'summary_QC', 'summary_nrev']
    for mod in fmri:
        columns_list.extend([f"{mod}_mean", f"{mod}_nrev"])
    df_merge_fmri = df_merge_fmri.reindex(columns=columns_list)

    df_merge_fmri = df_merge_fmri.round(decimals=3)

    # Ensure that all *_nrev columns are set to integer except *_summary_nrev
    cols_to_int = [
        c for c in df_merge_fmri.columns
        if c.endswith('_nrev') and not c.endswith('summary_nrev')
        ]

    df_merge_fmri[cols_to_int] = df_merge_fmri[cols_to_int].astype('int') 

    # Prepend table name to all column headers except first 3
    new_columns = df_merge_fmri.columns[:3].tolist() + [f'img_brainswipes_xcpd_{hash}_bold_' + col for col in df_merge_fmri.columns[3:]]
    df_merge_fmri.columns = new_columns

    # Save combined TSV
    df_merge_fmri.to_csv(f'OUT/img_brainswipes_xcpd_{hash}_bold.tsv', index=False, na_rep='NA', sep='\t')

