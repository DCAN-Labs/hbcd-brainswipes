import os
import pandas as pd
import json
from datetime import datetime

# Example anat: sub-X_ses-X_run-1_space-MNI152NLin6Asym_desc-SagittalInsulaTemporalHippocampalSulcus_T2w
# Example bold: sub-X_ses-X_task-rest_dir-PA_run-1_space-MNI152NLin6Asym_desc-T2wOnTaskBrainSwipes_bold

workdir='/home/feczk001/shared/projects/HBCD_QC'
input_csv=f'{workdir}/src/BrainSwipes_flagged.csv'
tempdir=f'{workdir}/temp'
outdir=f'{workdir}/out'
df=pd.read_csv(input_csv)

bucket_src='/midb-hbcd-prerelease-bids/assembly_bids'

# Read in csv file and strip the name from the flagged_message column
df=pd.read_csv(input_csv)
df['comment'] = df['comment'].str.split(':').str[1].str.strip()

if not os.path.exists(tempdir):
    os.makedir(tempdir)

# Create empty lists
seriesUID=[]
to_drop = [] # for subject data that doesn't exist on s3

for index, row in df.iterrows():
    # Parse subject and session IDs
    sub = row['scan'].split('_')[0]
    ses = row['scan'].split('_')[1]
    mod_filter = row['scan'].split('_')[2]

    # Parse the modality, scan type, and run number
    if 'run' in mod_filter:
        mod ='anat'
        run = row['scan'].split('_')[2]
        anat = row['scan'].split('_')[-1] # last element, T1w or T2w
        json_s3=f's3://{bucket_src}/{sub}/{ses}/{mod}/{sub}_{ses}_{run}_{anat}.json'

    else:
        mod='func'
        run = row['scan'].split('_')[4]
        json_s3=f's3://{bucket_src}/{sub}/{ses}/{mod}/{sub}_{ses}_task-rest_dir-PA_{run}_bold.json'

    # Download json to temp dir
    filename=os.path.basename(json_s3)
    json_temp=f'{tempdir}/{filename}'
    if not os.path.exists(json_temp):
        cmd=f's3cmd sync {json_s3} {tempdir}/'
        os.system(cmd)

    # Load JSON data and parse SeriesInstanceUID
    if os.path.exists(json_temp):
        with open(json_temp, 'r') as f:
            json_tmp = f.read()
        data = json.loads(json_tmp)
        UID = data['SeriesInstanceUID']
        seriesUID.append(UID)
    else:
        print(f"File {json_temp} does not exist. Skipping...")
        to_drop.append(index)

# Update DataFrame with SeriesInstanceUID and save to csv
df = df.drop(to_drop).reset_index(drop=True)

df_out=pd.DataFrame()
df_out['SeriesInstanceUID'] = seriesUID
df_out['comment'] = df['comment']
df_out['scan'] = df['scan']

date = datetime.now().strftime("%m-%d-%Y")
df_out.to_csv(f'{outdir}/BrainSwipes_Flagged_{date}.csv', index=False)




