import os
import pandas as pd
import json
from datetime import datetime

# Example anat: sub-X_ses-X_run-1_space-MNI152NLin6Asym_desc-SagittalInsulaTemporalHippocampalSulcus_T2w
# Example bold: sub-X_ses-X_task-rest_dir-PA_run-1_space-MNI152NLin6Asym_desc-T2wOnTaskBrainSwipes_bold

# HARDCODE WARNING
aws_access_key='********'
aws_secret_key='********'

workdir='/home/feczk001/shared/projects/HBCD'
input_csv='/home/feczk001/shared/projects/HBCD/BrainSwipes_flagged.csv'
tempdir='/home/feczk001/shared/projects/HBCD/temp'

bucket_src='/midb-hbcd-prerelease-bids/assembly_bids'
df=pd.read_csv(input_csv)

if not os.path.exists(tempdir):
    os.makedir(tempdir)

# Create empty lists
# runID=[] don't need this for final outputs I don't think
seriesUID=[]

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
    cmd=f's3cmd {json_s3} {tempdir}/ > /dev/null 2>&1'
    os.system(cmd)

    # Load JSON data and parse SeriesInstanceUID
    data = json.loads(json_temp)
    UID = data['SeriesInstanceUID']
    seriesUID.append(UID)

# Update DataFrame with SeriesInstanceUID and save to csv
df['SeriesInstanceUID'] = seriesUID
date = datetime.now().strftime("%Y-%m-%d")
df.to_csv(f'BrainSwipes_Flagged_{date}.csv', index=False)




