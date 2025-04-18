import pandas as pd

# Read the CSV file into a DataFrame
df = pd.read_csv('files/BrainSwipes-HBCDprocessed-results.csv')  # Replace with your actual filename

# Count how many times each value appears in the 'count' column
count_distribution = df['count'].value_counts().sort_index()

# Save out counts
count_distribution.to_csv('counts_distribution.csv', index=False)
