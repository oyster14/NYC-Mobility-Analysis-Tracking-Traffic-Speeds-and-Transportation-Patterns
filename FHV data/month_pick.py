import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

file_path = './month_pickup.txt'
data = []

with open(file_path, 'r') as file:
    lines = file.readlines()

for line in lines:
    year, month, location_id, frequency = line.strip().split('\t')
    if location_id != 'NULL':
        data.append({
            'Year': int(year),
            'Month': int(month),
            'LocationID': int(float(location_id)),
            'Frequency': int(frequency)
        })

df = pd.DataFrame(data)

top_pickup_locations = df.groupby('Month').apply(lambda x: x.nlargest(5, 'Frequency')).reset_index(drop=True)

sns.set(style="whitegrid")
fig, axes = plt.subplots(nrows=1, ncols=12, figsize=(24, 6), sharey=True)
colors = sns.color_palette("viridis", 5)

for i in range(1, 13):
    month_df = top_pickup_locations[top_pickup_locations['Month'] == i].sort_values(by='Frequency', ascending=False)
    sns.barplot(x='LocationID', y='Frequency', data=month_df, ax=axes[i-1], palette=colors)
    axes[i-1].set_title(f'Month {i}', fontsize=14)
    axes[i-1].set_xlabel('Location ID', fontsize=12)
    axes[i-1].set_ylabel('Frequency' if i == 1 else '', fontsize=12)
    axes[i-1].tick_params(labelsize=10)

plt.suptitle('Top 5 Frequent Pickup Locations for Each Month in 2021', fontsize=16)
plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.show()
