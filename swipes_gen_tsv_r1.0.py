import pandas as pd
import argparse
import numpy as np
import json

def main():
    parser = argparse.ArgumentParser(description='Convert the BrainSwipes CSV results file to a TSV file following HBCD specs.')
    parser.add_argument('input_csv', type=str, help='Path to the input CSV file')
    args = parser.parse_args()

    df = pd.read_csv(args.input_csv)

    tx = "T2w" #R1.0 only includes T2w
    smri = [f"AnatOnAtlasBrainSwipes_{tx}", f"AtlasOnAnatBrainSwipes_{tx}", f"AxialBasalGangliaPutamen_{tx}", f"AxialSuperiorFrontal_{tx}", f"CoronalCaudateAmygdala_{tx}", f"CoronalOrbitoFrontal_{tx}", f"CoronalPosteriorParietalLingual_{tx}", f"SagittalInsulaFrontoTemporal_{tx}", f"SagittalInsulaTemporalHippocampalSulcus_{tx}"]
   
    df_smri = pd.DataFrame()  # Create smri DataFrame

    ## Patient ID, Sub ID, Run
    df_smri['participant_id'] = df['sample'].str.split('_').str[0]
    df_smri['session_id'] = df['sample'].str.split('_').str[1]
    df_smri['run_id'] = df['sample'].str.split('_').str[2]

    ## Temporary columns
    df_smri['temp_mean'] = df['aveVote']
    df_smri['temp_nrev'] = df['count']
    df_smri['temp_mod'] = df['sample'].str.split('_').str[4] + '_' + df['sample'].str.split('_').str[5]

    # Filter out func rows
    df_smri = df_smri[df_smri['run_id'].str.contains('run')]

    # Filter for each modality type and merge to output df
    df_merge = pd.DataFrame(columns=["participant_id", "session_id", "run_id"])
    for mod in smri:
        df_filt = df_smri[df_smri['temp_mod'].str.contains(mod)]
        df_filt = df_filt.drop('temp_mod', axis=1)
        df_filt.rename(columns={"temp_mean": f"{mod}_mean"}, inplace=True)
        df_filt.rename(columns={"temp_nrev": f"{mod}_nrev"}, inplace=True)

        df_merge=pd.merge(df_merge, df_filt, on=["participant_id", "session_id", "run_id"], how="outer")

    # Adjust values after merging - set nrev to 0 if NaN 
    for index, row in df_merge.iterrows():
        for mod in smri:
            if pd.isna(row[f"{mod}_nrev"]):
                df_merge.at[index, f"{mod}_nrev"] = 0

    # Create summary_QC and summary_nrev columns from average across mean QC and mean nrev columns
    mean_columns = [col for col in df_merge.columns if "_mean" in col]
    nrev_columns = [col for col in df_merge.columns if "_nrev" in col]

    # TO DO in future: somehow indicate whether all scans have been swiped on, or make average QC NA if not
    df_merge['summary_QC'] = df_merge[mean_columns].mean(axis=1)
    df_merge['summary_nrev'] = df_merge[nrev_columns].mean(axis=1)

    # Ensure column order is correctly set
    columns_list=["participant_id", "session_id", "run_id", "summary_QC", "summary_nrev"]
    for mod in smri:
        columns_list.append(f"{mod}_mean")
        columns_list.append(f"{mod}_nrev")
    df_merge = df_merge.reindex(columns=columns_list)

    # Drop 'Txw_' from column headers as this will be redundant after prepending table name 
    df_merge.columns = df_merge.columns.str.replace(f'{tx}_', '')

    # Prepend table name to all column headers except first 3
    new_columns = df_merge.columns[:3].tolist() + [f'img_brainswipes_xcpd-{tx}_' + col for col in df_merge.columns[3:]]
    df_merge.columns = new_columns
    df_merge=df_merge.round(decimals=3)

    # Save
    df_merge.to_csv(f'img_brainswipes_xcpd_{tx}.tsv', index=None, na_rep='NA', sep='\t')

    # FMRI
    df_fmri = pd.DataFrame() # fmri DataFrame

    ## Patient ID, Sub ID, Run
    df_fmri['participant_id'] = df['sample'].str.split('_').str[0]
    df_fmri['session_id'] = df['sample'].str.split('_').str[1]
    df_fmri['run_id'] = df['sample'].str.split('_').str[4]

    ## Temporary columns
    df_fmri['temp_mean'] = df['aveVote']
    df_fmri['temp_nrev'] = df['count']
    df_fmri['temp_mod'] = df['sample'].str.split('_').str[6] + '_' + df['sample'].str.split('_').str[7]

    # Filter out anat rows
    df_fmri = df_fmri[df_fmri['run_id'].str.contains('run')]
    #df_fmri = df_fmri[df_fmri['temp_mod'].str.contains('bold')]

    # Filter for each modality type and merge to output df
    df_merge = pd.DataFrame(columns=["participant_id", "session_id", "run_id"])
    fmri= [f"{tx}OnTaskBrainSwipes_bold", f"TaskOn{tx}BrainSwipes_bold"]

    for mod in fmri:
        df_filt = df_fmri[df_fmri['temp_mod'].str.contains(mod)]
        df_filt = df_filt.drop('temp_mod', axis=1)
        df_filt.rename(columns={"temp_mean": f"{mod}_mean"}, inplace=True)
        df_filt.rename(columns={"temp_nrev": f"{mod}_nrev"}, inplace=True)

        df_merge=pd.merge(df_merge, df_filt, on=["participant_id", "session_id", "run_id"], how="outer")

    # Adjust values after merging - set nrev to 0 if NaN 
    for index, row in df_merge.iterrows():
        for mod in fmri:
            if pd.isna(row[f"{mod}_nrev"]):
                df_merge.at[index, f"{mod}_nrev"] = 0

    # Create summary_QC and summary_nrev columns from average across mean QC and mean nrev columns
    mean_columns = [col for col in df_merge.columns if "_mean" in col]
    nrev_columns = [col for col in df_merge.columns if "_nrev" in col]

    # TO DO in future: somehow indicate whether all scans have been swiped on, or make average QC NA if not
    df_merge['summary_QC'] = df_merge[mean_columns].mean(axis=1)
    df_merge['summary_nrev'] = df_merge[nrev_columns].mean(axis=1)

    # Ensure column order is correctly set
    columns_list=["participant_id", "session_id", "run_id", "summary_QC", "summary_nrev"]
    for mod in fmri:
        columns_list.append(f"{mod}_mean")
        columns_list.append(f"{mod}_nrev")
    df_merge = df_merge.reindex(columns=columns_list)

    # Drop 'Txw' from column headers as this will be redundant after prepending table name 
    df_merge.columns = df_merge.columns.str.replace(f'{tx}', '')

    # Drop 'bold_' from column headers as this will be redundant after prepending table name 
    df_merge.columns = df_merge.columns.str.replace('bold_', '')

    # Prepend table name to all column headers except first 3
    new_columns = df_merge.columns[:3].tolist() + ['img_brainswipes_xcpd-bold_' + col for col in df_merge.columns[3:]]
    df_merge.columns = new_columns
    df_merge=df_merge.round(decimals=3)

    # Save
    df_merge.to_csv('img_brainswipes_xcpd_bold.tsv', index=None, na_rep='NA', sep='\t')

if __name__ == "__main__":
    main()

