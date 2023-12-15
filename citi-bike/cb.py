# referenced https://justinmorganwilliams.medium.com/how-to-make-a-time-lapse-heat-map-with-folium-using-nyc-bike-share-data-1ccd2e32c2e3
import folium

def generateBaseMap(loc, zoom=12, tiles='Stamen Toner', crs='ESPG2263'):
    '''
    Function that generates a Folium base map
    Input location lat/long
    Zoom level default 12
    Tiles default to Stamen Toner
    CRS default 2263 for NYC
    '''
    return folium.Map(location=loc, 
                      control_scale=True, 
                      zoom_start=zoom)
  
nyc = [40.7400, -73.985880] # generic nyc lat/lon in list format
base_map = generateBaseMap(nyc) # pass lat/lon to function

import pandas as pd

file_names = ['2020-citibike-tripdata.csv',]

dataframes = [pd.read_csv('./2020/' + file, parse_dates=['starttime', 'stoptime']) for file in file_names]

df = pd.concat(dataframes, ignore_index=True)

# import HeatMap plugin
from folium.plugins import HeatMap

# add data to basemap which we created above with custom function
HeatMap(data=lst, radius=10).add_to(base_map);

# save base map as .html
base_map.save('./bike_station_HeatMap.html')


import pandas as pd
from datetime import datetime
from folium.plugins import HeatMapWithTime

# create new base map for heat map with time
base_map_2 = generateBaseMap(nyc)


# create a more meaningful index for heat map with time
start = datetime(2020,1,1,0)
end = datetime(2020,1,1,23)
daterange = pd.date_range(start=start,end=end, periods=24) #use pandas daterange function to generate date range object

time_index = [d.strftime("%I:%M %p") for d in daterange] # format time with AM/PM

# instantiate HeatMapWithTime
HeatMapWithTime(df_hour_list,radius=11,
                index=time_index,
                gradient={0.1: 'blue', 0.5: 'lime', 0.7: 'orange', 1: 'red'}, 
                min_opacity=0.4, 
                max_opacity=0.8, 
                use_local_extrema=True)\
                .add_to(base_map_2)

# save as html
base_map_2.save('./heatmapwithtime_bikeshare.html')

hours = list(range(24))  # Hours from 0 to 23
hour_ride_count = []

for hour in df['hour'].sort_values().unique(): 
    hour_df = df.loc[df['hour'] == hour]
    hour_ride_count.append(hour_df.shape[0])


import matplotlib.pyplot as plt

# Plot
plt.figure(figsize=(10, 6))
plt.plot(hours, hour_ride_count, marker='o')  # Line plot
# plt.bar(hours, values)  # Uncomment this and comment the line above for a bar chart
# plt.title('Hourly Data Representation')
plt.xlabel('Hour of the Day')
plt.ylabel('Ride Count')
plt.xticks(hours)
plt.grid(True)
plt.save('./day_count.png')