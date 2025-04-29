import os
import json
import pandas as pd
from datetime import datetime

workdir='/home/feczk001/shared/projects/HBCD_QC'
input_csv=f'{workdir}/src/BIBSNet.csv'
tempdir=f'{workdir}/temp'
outdir=f'{workdir}/out'
df=pd.read_csv(input_csv)

# First figure out the run ID for the T1w/T2w
bucket_bibsnet_src = 'midb-hbcd-main-pr-derivatives-archive' # source bucket for intermediate BIBSNet pipeline output files copied to PrimeNeuro bucket 
bucket_rawbids = 'midb-hbcd-main-pr' # bucket with raw BIDS data (deidentified so will match subject IDs in bibsnet src bucket)

# Create empty lists
seriesUID=[]

if not os.path.exists(tempdir):
    os.mkdir(tempdir)
if not os.path.exists(outdir):
    os.mkdir(outdir)

for index, row in df.iterrows():
    sub = row['subject']
    ses = row['session']
    tx= row['tx']

    json_s3=f's3://{bucket_bibsnet_src}/derivatives/{ses}/bibsnet_work/{sub}/derivatives/bibsnet/{sub}/{ses}/anat/{sub}_{ses}_space-{tx}_desc-aseg_dseg.json'
    json_tmp=f"{tempdir}/{sub}_{ses}_space-{tx}_desc-aseg_dseg.json"

    # Download json to temp dir
    if not os.path.exists(json_tmp):
        cmd=f's3cmd sync {json_s3} {tempdir}/'
        os.system(cmd)

    # Load JSON data and parse SpatialReference
    with open(json_tmp, 'r') as f:
        json_tmp = f.read()
    data = json.loads(json_tmp)
    spatial_ref_file = os.path.basename(data['SpatialReference']) # sub-XXX_ses-XXX_run-X_T<1|2>w.nii.gz
    
    # Replace .nii.gz with .json
    spatial_ref_file_json = spatial_ref_file.replace('.nii.gz', '.json')

    # Next pull corresponding raw BIDS anat file to temp dir and parse SeriesInstanceUID
    json_s3=f"s3://{bucket_rawbids}/assembly_bids/{sub}/{ses}/anat/{spatial_ref_file_json}"
    json_tmp=f"{tempdir}/{spatial_ref_file_json}"

    if not os.path.exists(json_tmp):
        cmd=f's3cmd sync {json_s3} {tempdir}/'
        os.system(cmd)

    # Load JSON data and parse SeriesInstanceUID value
    with open(json_tmp, 'r') as f:
        json_tmp = f.read()
    data = json.loads(json_tmp)
    UID = data['SeriesInstanceUID']
    seriesUID.append(UID)

# Create new DataFrame with SeriesInstanceUID and save to csv
df_out=pd.DataFrame()
df_out['SeriesInstanceUID'] = seriesUID
df_out['session'] = df['session']
df_out['tx'] = df['tx']
df_out['QU_Motion_current'] = df['QU_Motion_current']

date = datetime.now().strftime("%m-%d-%Y")
df_out.to_csv(f'{outdir}/QU_Motion_Rescore_Request_{date}.csv', index=False)
