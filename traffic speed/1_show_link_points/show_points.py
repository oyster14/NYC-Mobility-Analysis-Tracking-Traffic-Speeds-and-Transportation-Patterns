import pandas as pd
import folium
import os


folder_path = './all_distinct'  


all_files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.csv')]
df_list = [pd.read_csv(file, header=None, names=['latitude', 'longitude']) for file in all_files]
combined_df = pd.concat(df_list, ignore_index=True)


m = folium.Map(location=[combined_df['latitude'].mean(), combined_df['longitude'].mean()], zoom_start=12)


for _, row in combined_df.iterrows():
    folium.Marker(location=[row['latitude'], row['longitude']]).add_to(m)


m.save('combined_map.html')  

