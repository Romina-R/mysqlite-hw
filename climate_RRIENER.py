#!/usr/bin/env python
# coding: utf-8

# In[2]:


get_ipython().run_line_magic('matplotlib', 'inline')
from matplotlib import style
style.use('fivethirtyeight')
import matplotlib.pyplot as plt


# In[3]:


import numpy as np
import pandas as pd
from sqlalchemy import inspect
import pymysql
pymysql.install_as_MySQLdb()


# In[4]:


import datetime as dt


# # Reflect Tables into SQLAlchemy ORM

# In[5]:


# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy import Column, Float, Integer, String


# In[6]:


engine = create_engine("sqlite:///Resources/hawaii.sqlite")


# In[7]:


# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)


# In[8]:


# We can view all of the classes that automap found
Base.classes.keys()


# In[9]:


# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station


# In[10]:


# Create our session (link) from Python to the DB
session = Session(engine)


# In[11]:


data= engine.execute("SELECT * FROM Measurement")
print(data.keys())
for record in data:
    print(record)


# In[12]:


data_station = engine.execute("SELECT * FROM Station")
print(data_station.keys())
for record in data_station:    
    print(record)


# # Exploratory Climate Analysis

# In[13]:


# Design a query to retrieve the last 12 months of precipitation data and plot the results

# Calculate the date 1 year ago from the last data point in the database
lastyear = dt.date(2017,8,23) - dt.timedelta(days=365)

# Perform a query to retrieve the data and precipitation scores
yearPrecip = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= lastyear).all()
yearPrecip

# Save the query results as a Pandas DataFrame and set the index to the date column
df = pd.DataFrame(yearPrecip, columns = ["date", "precipitation"])
#df1 = df.set_index(df["date"])
#df2 = df1["precipitation"]
# Sort the dataframe by date
df3 = df.sort_values('date').reset_index()

df3.head()


# In[14]:


# Use Pandas Plotting with Matplotlib to plot the data
plt.bar(df3.date, df3.precipitation, color="b")
plt.title("Precipitation")
plt.xlabel("Date")


# In[15]:


inspector = inspect(engine)
inspector.get_table_names()


# ![precipitation](Images/precipitation.png)

# In[16]:


# Use Pandas to calcualte the summary statistics for the precipitation data
df3["precipitation"].describe()


# ![describe](Images/describe.png)

# In[17]:


# Design a query to show how many stations are available in this dataset?
measurement_query = session.query(Measurement.station, Measurement.date, Measurement.prcp, Measurement.tobs)
measurement_df = pd.DataFrame(measurement_query, columns=["station","date", "precipitation", "tobs"])
stationNumber = len(measurement_df["station"].unique())
stationNumber


# In[48]:


# What are the most active stations? (i.e. what stations have the most rows)?
# List the stations and the counts in descending order.
rowcount = measurement_df["station"].value_counts()
print(rowcount)
print("------")
rowcount_df = pd.DataFrame(rowcount)

#maxRows
max_ = rowcount_df.loc[rowcount_df["station"] == rowcount_df["station"].max()]
print(max_)


# In[19]:


# Using the station id from the previous query, calculate the lowest temperature recorded, 
# highest temperature recorded, and average temperature most active station?


#got a little carried away with dataframes.... gettting back on track now
session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)).    filter(Measurement.station == 'USC00519281').all()


# In[22]:


# Choose the station with the highest number of temperature observations.
# Query the last 12 months of temperature observation data for this station 
# and plot the results as a histogram
year_st81 = session.query(Measurement.station, Measurement.date, Measurement.tobs).    filter(Measurement.station == 'USC00519281').filter(Measurement.date >= lastyear).all()

st81_df = pd.DataFrame(year_st81, columns = ["station","date", "tobs"])

# Sort the dataframe by date
st81_df.sort_values('date').reset_index(inplace=True)

st81_df.head()


# In[25]:


st81_df.plot.hist(st81_df.tobs, bins=12)

plt.title("Histogram")
plt.ylabel("Frequency")


# ![precipitation](Images/station-histogram.png)

# In[28]:


# This function called `calc_temps` will accept start date and end date in the format '%Y-%m-%d' 
# and return the minimum, average, and maximum temperatures for that range of dates
def calc_temps(start_date, end_date):
    """TMIN, TAVG, and TMAX for a list of dates.
    
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVE, and TMAX
    """
    
    return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()

# function usage example
print(calc_temps('2012-02-28', '2012-03-05'))


# In[34]:


# Use your previous function `calc_temps` to calculate the tmin, tavg, and tmax 
# for your trip using the previous year's data for those same dates.
print(calc_temps('2017-02-28', '2017-03-05'))
tmin, tavg, tmax = calc_temps('2017-02-28', '2017-03-05')[0]
print(tmin, tavg, tmax)


# In[44]:


# Plot the results from your previous query as a bar chart. 
# Use "Trip Avg Temp" as your Title
# Use the average temperature for the y value
# Use the peak-to-peak (tmax-tmin) value as the y error bar (yerr)
plt.title("Trip Avg Temp")
plt.ylabel("Temp")
plt.bar(1,tavg, yerr=tmax-tmin, alpha=0.5, color='orange')


# In[45]:


# Calculate the total amount of rainfall per weather station for your trip dates using the previous year's matching dates.
# Sort this in descending order by precipitation amount and list the station, name, latitude, longitude, and elevation

start_date = '2017-02-28'
end_date = '2017-03-05'

sel = [Station.station, Station.name, Station.latitude, 
       Station.longitude, Station.elevation, func.sum(Measurement.prcp)]

results = session.query(*sel).    filter(Measurement.station == Station.station).    filter(Measurement.date >= start_date).    filter(Measurement.date <= end_date).    group_by(Station.name).order_by(func.sum(Measurement.prcp).desc()).all()
print(results)


# ## Optional Challenge Assignment

# In[ ]:


# Create a query that will calculate the daily normals 
# (i.e. the averages for tmin, tmax, and tavg for all historic data matching a specific month and day)

def daily_normals(date):
    """Daily Normals.
    
    Args:
        date (str): A date string in the format '%m-%d'
        
    Returns:
        A list of tuples containing the daily normals, tmin, tavg, and tmax
    
    """
    
    sel = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
    return session.query(*sel).filter(func.strftime("%m-%d", Measurement.date) == date).all()
    
daily_normals("01-01")


# In[ ]:


# calculate the daily normals for your trip
# push each tuple of calculations into a list called `normals`

# Set the start and end date of the trip

# Use the start and end date to create a range of dates

# Stip off the year and save a list of %m-%d strings

# Loop through the list of %m-%d strings and calculate the normals for each date


# In[ ]:


# Load the previous query results into a Pandas DataFrame and add the `trip_dates` range as the `date` index


# In[ ]:


# Plot the daily normals as an area plot with `stacked=False`

