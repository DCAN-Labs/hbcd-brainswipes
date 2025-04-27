import os
import json
import pandas as pd
from datetime import datetime

workdir='/home/feczk001/shared/projects/HBCD'
input_csv='/home/feczk001/shared/projects/HBCD/sublist.csv'
tempdir='/home/feczk001/shared/projects/HBCD/temp'
df=pd.read_csv(input_csv)

# HARDCODE WARNING
aws_access_key='********'
aws_secret_key='********'

# First figure out the run ID for the T1w/T2w
bucket_bibsnet_src = 'midb-hbcd-main-pr-derivatives-archive' # source bucket for intermediate BIBSNet pipeline output files copied to PrimeNeuro bucket 
bucket_rawbids = 'midb-hbcd-main-pr' # bucket with raw BIDS data (deidentified so will match subject IDs in bibsnet src bucket)

# Create empty lists
# runID=[] don't need this for final outputs I don't think
seriesUID=[]

if not os.path.exists(tempdir):
    os.makedir(tempdir)

for index, row in df.iterrows():
    print("\n")
    sub = row['subject']
    ses = row['session']
    tx= row['tx']

    json_s3=f's3://{bucket_bibsnet_src}/derivatives/{ses}/bibsnet_work/{sub}/derivatives/bibsnet/{sub}/{ses}/{sub}_{ses}_space-{tx}_desc-aseg_dseg.json'
    json_tmp=f"{tempdir}/{sub}_{ses}_space-{tx}_desc-aseg_dseg.json"

    # Download json to temp dir
    cmd=f's3cmd {json_s3} {json_tmp} > /dev/null 2>&1'
    os.system(cmd)

    # Load JSON data and parse SpatialReference
    data = json.loads(json_tmp)
    spatial_ref_file = data['SpatialReference'] # sub-XXX_ses-XXX_run-X_T<1|2>w.nii.gz

    # Replace .nii.gz with .json
    spatial_ref_file_json = spatial_ref_file.replace('.nii.gz', '.json')

    # Next pull corresponding raw BIDS anat file to temp dir and parse SeriesInstanceUID
    json_s3=f"s3://{bucket_rawbids}/assembly_bids/{sub}/{ses}/anat/{spatial_ref_file_json}"
    json_tmp=f"{tempdir}/{spatial_ref_file_json}"

    cmd=f's3cmd {json_s3} {json_tmp} > /dev/null 2>&1'
    os.system(cmd)

    # Load JSON data and parse SeriesInstanceUID value
    data = json.loads(json_tmp)
    UID = data['SeriesInstanceUID']
    seriesUID.append(UID)

# Update DataFrame with SeriesInstanceUID and save to csv
df['SeriesInstanceUID'] = seriesUID
date = datetime.now().strftime("%Y-%m-%d")
df.to_csv(f'QU_Motion_Rescore_Request_{date}.csv', index=False)
