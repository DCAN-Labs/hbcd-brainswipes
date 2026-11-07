import pandas as pd
import os

# This script parses the BrainSwipes chat info to assign 0 post-hoc to flagged images (not able to rate images once flagged)

# Pull chat information from Google Sheet
sheet_id = "1P6QFJaZjb-F5roWkzQXkoGFW1E95t9rge6RfNmmKozc"
px_list = ["p1", "p2"]
sheet_gid_p1 = "898005270"
sheet_gid_p2 = "1197223882"

# FUNCTIONS
def load_and_filter(sheet_id, sheet_gid):

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={sheet_gid}"
    df = pd.read_csv(url, dtype=str, header=None)

    # Remove completely empty rows
    df = df.dropna(how="all").reset_index(drop=True)

    # Transform to move every other row into a new column
    user = df.iloc[::2].reset_index(drop=True)
    assistant = df.iloc[1::2].reset_index(drop=True)

    # Combine into a single dataframe
    output = pd.DataFrame({
        "File": user.iloc[:, 0],
        "Chat": assistant.iloc[:len(user), 0]
    })

    # Drop rows where Chat does not include "lmoore"; Drop rows where Chat includes "Pass"
    output = output[output["Chat"].str.contains("lmoore", na=False)]
    output = output[~output["Chat"].str.contains("Pass", na=False)]

    # Drop Chat column
    output = output.drop(columns=["Chat"])

    return output

# WORK
for px in px_list:
    if px == "p1":
        df_chat = load_and_filter(sheet_id, sheet_gid_p1)
    elif px == "p2":
        df_chat = load_and_filter(sheet_id, sheet_gid_p2)

    df=pd.DataFrame()
    df["sample"] = df_chat["File"].str.split(",").str[0]
    df["aveVote"] = 0
    df["count"] = 1

    df.to_csv(f"data/assign-0-posthoc_{px}.csv", index=False)



