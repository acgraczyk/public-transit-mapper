# -*- coding: utf-8 -*-
"""
Created on Sun Jul 12 10:36:32 2020

@author: Andrew
"""
import requests
import pandas as pd
import time
import re
import dill
from collections import Counter, defaultdict
import numpy as np
from datetime import datetime
from datetime import timedelta
import geopandas
from geopy.geocoders import Nominatim
import math
import folium
######
#Compute maximum reliable transit distance from a location
####in Toronto, 250m~=.00225 Lat, .00307 Lon


###Need to get: stops in order for each route, cross-referenced with stop locations.
##From address: look up nearest stops
#From stops get routes, timetables/reliabilities for following stops on each route direction.
#For each stop, call nearby transferrable routes (on other lines)
url='http://webservices.nextbus.com/service/publicXMLFeed'

columns=['Vehicle_ID','routeTag', 'dirTag','lat','lon','time','secSinceReport','lastTime']
agency='ttc'
#route='5'
#parameters_r={'command': 'routeConfig','a':agency,'r':route}
#parameters_s={'command': 'schedule','a':agency,'r':route}
#text_r=requests.get(url,params=parameters_r).text
#text_s=requests.get(url,params=parameters_s).text
#(?<=stop\stag=")(.+)(?=">[A-Z])

#Create a dictioary with levels: class, direction, tags,times,time_diffs as DataFrame
def get_stop_schedule(text_s):
    grand_reg=re.findall('(?<=serviceClass=")(.+)(?="\s)|(?<=direction=")(.+)(?=">)|(?<=stop\stag=")(.+)(?="\se)(?!.+-1)|(?<=[0-9][0-9][0-9][0-9][0-9][0-9]">)(.+)(?=<\/s)',text_s)
    route_sched_dict=defaultdict(dict)
    count=0
    ind=0
    while ind<len(grand_reg):
#        route_sched_dict[grand_reg[ind][0]]=dict()
        ind+=1
        route_sched_dict[grand_reg[ind-1][0]][grand_reg[ind][1]]=pd.DataFrame({'Tag':[],
                                                                 'Time':[]})
        ind+=1
        stop_tags_list=[]
        stop_times_list=[]
        count=0
        while ind<len(grand_reg) and not grand_reg[ind][0] :
            if grand_reg[ind][2] not in stop_tags_list:
                stop_tags_list.append(grand_reg[ind][2])
                ind+=1
                count+=1
                stop_times_list.append(grand_reg[ind][3])
                ind+=1
                count+=1
            else:
                ind+=2
                count+=2
        route_sched_dict[grand_reg[ind-count-2][0]][grand_reg[ind-count-1][1]]=pd.DataFrame({'Tag':stop_tags_list,
                                                                 'Time':stop_times_list})
    route_sched_time_dict=get_time_costs(route_sched_dict)
    return route_sched_time_dict

#gets time costs for going to a stop from the previous stop on a route.
def get_time_costs(dict_obj):
    for el in dict_obj:
        for di in dict_obj[el]:
            time_diff_list=[0]*len(dict_obj[el][di])
            times=[datetime.strptime(element,'%H:%M:%S') for element in list(dict_obj[el][di]['Time'])]
            dict_obj[el][di]['datetimes']=times
            dict_obj[el][di].sort_values(by='datetimes')
            times=sorted(times)
            for i in range(1,len(time_diff_list)):
                time_diff_list[i]=int((times[i]-times[i-1]).seconds/60)
            
            dict_obj[el][di]['Time_diff']=time_diff_list   
    return dict_obj

#returns dataframe of stop positions, titles, tags
def get_stop_positions(text_r):
    stop_title_r=re.findall('(?<=title=")(.+)(?="\slat)',text_r)
    stop_title_r=stop_title_r[1:] 
    stop_tags=re.findall('(?<=<stop\stag=")(.+)(?="\st)',text_r)
    stop_lat=re.findall('"\slat="(.+)(?="\sl)',text_r)
    stop_lon=re.findall('..." lat="[0-9]+.[0-9]+" lon="(.[0-9]+.[0-9]+)"',text_r)
    stop_positions=pd.DataFrame({'Tag': stop_tags, 'Title': stop_title_r,
                                 'Lat': stop_lat, 'Lon': stop_lon})  
    return stop_positions

#Get tuples of direction tags, names
def get_dir_tags(text_r):
    dir_tags=re.findall('(?<=direction tag=")(.+)(?="\st)',text_r)
    dir_names=re.findall('(?<=name=")(.+)(?="\su)',text_r)
    dir_tup=[(dir_tags[i],dir_names[i]) for i in range(len(dir_tags))]
    return dir_tup


#appends stop position data to dict of schedule stop times/time differences by class and direction
def add_stop_post(sched_dict,stop_df):
    for el in sched_dict:
        for di in sched_dict[el]:
            sched_dict[el][di].merge(stop_df,on='Tag',how='inner')
    return sched_dict

#returns dictionary with levels: class,direction,tags/times/time_diffs/positions in a DataFrame 
#also returns dataframe of stop tags, locations, names for merger into All_Stops
def get_stop_data(url,parameters_s,parameters_r):
    text_s=requests.get(url,params=parameters_s).text  
    text_r=requests.get(url,params=parameters_r).text

    route_positions=get_stop_positions(text_r)
    route_schedule=get_stop_schedule(text_s)
    routes_times=add_stop_post(route_schedule,route_positions)
    
    return routes_times, route_positions

def get_vehicle_data(url,base,basis,route):
    
    base_diff=base-basis
    time_steps=int(1.0*base_diff/60)
    time_limit=range(time_steps)
    time_list=[]
    for i in time_limit:
        time_list.append(str(base-i*60))
    
    columns=['Vehicle_ID','routeTag', 'dirTag','lat','lon','time','secSinceReport','lastTime']
    df_vehicle=pd.DataFrame(columns=columns)
    for s in time_list:
        parameters_v={'command': 'vehicleLocations', 'a': 'ttc', 'r':route, 
                      'time': s}  
    
        data=requests.get(url,params=parameters_v).text
    
        veh=re.findall('(?<=vehicle id=)"[0-9]+"\s',data)
        route=re.findall('(?<=routeTag=)"[0-9]+"\s',data)
        dir_tag=re.findall('(?<=dirTag=")(.+)(?="\slat)',data)
        lat=re.findall('(?<=lat=")([0-9]+.[0-9]+)',data)
        lon=re.findall('(?<=lon=")(.[0-9]+.[0-9]+)',data)
        secs=re.findall('(?<=secsSinceReport=)"[0-9]+"\s',data)
        lastTime=re.findall('(?<=lastTime\stime=)"[0-9]+"',data)
    
        if veh:        
            lastTimes=[lastTime]*len(veh)
            times=[s]*len(veh)
            vehicle_data=pd.DataFrame(list(zip(veh,route,dir_tag,lat,lon,times,secs,lastTimes)),columns=columns)
            pd.concat([df_vehicle,vehicle_data],ignore_index=True)
        
    return df_vehicle

#returns a dataframe of all agency routes
def get_route_list(url,agency):
    parameters={'command': 'routeList', 'a':agency}
    routes=requests.get(url,params=parameters).text
    route_tags=re.findall('(?<=route\stag=")(.+)(?="\s)',routes)
    route_names=re.findall('(?<=title=")(.+)(?=")',routes)
    
    columns=['Route_Tags','Route_Names']
    Agency_Routes_df=pd.DataFrame(list(zip(route_tags,route_names)),columns=columns)
    return Agency_Routes_df

#merge stop data (tags/positions) for all routes
def merge_route_stops(list_of_routes,list_of_dfs):
    for i in range(len(list_of_dfs)):
        list_of_dfs[i]['route']=list_of_routes[i]   
    return pd.concat(list_of_dfs,ignore_index=True)

#function to store route info
def route_store(url,agency,agency_routes):
    for route in agency_routes['Route_Tags']:
        parameters_r={'command': 'routeConfig','a':agency,'r':route}
        parameters_s={'command': 'schedule','a':agency,'r':route}
        
        route_data=get_stop_data(url,parameters_s,parameters_r)
        dill.dump(route_data,open('route_data_{}.pkd'.format(route),'wb'))

#a simple function to store all stops for an agency in a .pkd file for easy retrieval.
def store_all_stops(all_stops,agency):
    np.save('all_stops_{}.npy'.format(agency),all_stops)
    
def store_transfer_dicts(transfer_dicts,agency):
    np.save('transfer_dict_{}.npy'.format(agency),transfer_dicts)
        
    
def store_route_dict(route_dict,agency):
    np.save('route_dict_{}.npy'.format(agency),route_dict)
#pre-calculate which stops are nearby enough for transferring for assembling dictionaries of 
#which stops are accessible from a given address. 




#get meter distance from lat/lon differences using Haversine formula
def distance_haversine(lat1,lon1,lat2,lon2):
    radius = 6371  # km
    lat1=float(lat1)
    lat2=float(lat2)
    lon1=float(lon1)
    lon2=float(lon2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) * math.sin(dlat / 2) +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) * math.sin(dlon / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = radius * c

    return d*1000

#Generate dictionary for transfers.  Can be adapted for different distances traveled on foot.
#Returns a dictionary of lists of tuples with station tags, distances (in 100 meter units) between a stop
#and other stops on different routes within a certain distance, specified by 'distance'.  Default distance
#is 200 m. 
def generate_transfer_dict(All_Stops_df,distance=250):
    dist_mod=1.0*distance/100  #Get distance in 100m units
    m_mark_base=.0009009 #approx lat/lon of 100 m.
    m_mark=dist_mod*m_mark_base    
    transfer_dict=defaultdict(list)
    
    for el in All_Stops_df['Tag'].unique():
 #       print('row: {}'.format(el))
        row=All_Stops_df.loc[All_Stops_df['Tag']==el]
        stat_lat=list(row['Lat'])[0]
        stat_lon=list(row['Lon'])[0]
        tag=el
        route=list(row['route'])[0]
        m_set=All_Stops_df.loc[(abs(All_Stops_df['Lat'].astype(float)-float(stat_lat))<=m_mark) & 
                         (abs(All_Stops_df['Lon'].astype(float)-float(stat_lon))<=m_mark) &
                         (All_Stops_df['route']!=route)]
        for s,stop in m_set.iterrows():
 #           print('stop: {}'.format(stop.Tag))
            #Get tag, average walking time for connectable stations.  Average walking speed ~1.4 m/s, so ~84 meters/minute. 
            transfer_dict[tag].append((stop.Tag,stop.route,distance_haversine(stat_lat,stat_lon,stop.Lat,stop.Lon)/84))
        
    return transfer_dict
            
         
        

def generate_vertices(All_Stops):
    east_bound=[]
    lat_cents=[]
    lon_cents=[]
    west_bound=[]
    north_bound=[]
    south_bound=[]
    stop_tags=[]
    route_tags=[]
    for ind, row in All_Stops.iterrows():
        east_bound.append(row.Lon-.002)
        west_bound.append(row.Lon+.002)
        north_bound.append(row.Lat+.002)
        south_bound.append(row.Lat-.002)
        stop_tags.append(row.Tag)
        route_tags.append(row.route)
        lat_cents.append(row.Lat)
        lon_cents.append(row.Lon)
    df_bound=pd.DataFrame(list(zip(stop_tags,route_tags,lat_cents,lon_cents,north_bound,south_bound,east_bound,west_bound)),
                          columns=['Tag','route','Lat','Lon','north_bound','south_bound','east_bound','west_bound'])
    return df_bound

def get_route_data(url,agency):
    route_list_df=get_route_list(url,agency)
    
    list_of_routes=list(route_list_df['Route_Tags'])
    
    route_dictionary=dict()
    route_stop_list=[]
    for route in list_of_routes:
        parameters_r={'command': 'routeConfig','a':agency,'r':route}
        parameters_s={'command': 'schedule','a':agency,'r':route}
        temp_dict, temp_df = get_stop_data(url,parameters_s,parameters_r)
        route_dictionary[route]=temp_dict
        route_stop_list.append(temp_df)
    All_Stops_df=merge_route_stops(list_of_routes,route_stop_list)
    store_all_stops(All_Stops_df,agency)
    
    transfer_dictionary=generate_transfer_dict(All_Stops_df)
    store_transfer_dicts(transfer_dictionary,agency)
    
    store_route_dict(route_dictionary,agency)
    
    return route_dictionary, All_Stops_df, transfer_dictionary
    
    
def test_get_route_data(url,agency):
    route_list_df=get_route_list(url,agency)
    
    list_of_routes=list(route_list_df['Route_Tags'])[:10]
    
    route_dictionary=dict()
    route_stop_list=[]
    for route in list_of_routes:
        parameters_r={'command': 'routeConfig','a':agency,'r':route}
        parameters_s={'command': 'schedule','a':agency,'r':route}
        temp_dict, temp_df = get_stop_data(url,parameters_s,parameters_r)
        route_dictionary[route]=temp_dict
        route_stop_list.append(temp_df)
    All_Stops_df=merge_route_stops(list_of_routes,route_stop_list)
#    store_all_stops(All_Stops_df,agency)
    
    transfer_dictionary=generate_transfer_dict(All_Stops_df)
#    store_transfer_dicts(transfer_dictionary,agency)
    
    store_route_dict(route_dictionary,agency)
    
    return route_dictionary, All_Stops_df, transfer_dictionary    

#initialize geopandas locator
def set_locator():
    return Nominatim(user_agent="myGeocoder")

#Get lat/lon of address using locator    
def get_Toronto_coords(add_string,locator):
    location=locator.geocode('{},Toronto, Canada'.format(add_string))
    return (location.latitude,location.longitude)
    
def near_stops(All_Stops,stat_coord,distance=100):
    stat_lat=stat_coord[0]
    stat_lon=stat_coord[1]
    dist_mod=1.0*distance/100
    m_mark_base=.0009009 #approx lat/lon of 100 m.
    m_mark=dist_mod*m_mark_base
    m_set=All_Stops.loc[(abs(All_Stops['Lat'].astype(float)-float(stat_lat))<=m_mark) & 
                    (abs(All_Stops['Lon'].astype(float)-float(stat_lon))<=m_mark)]
    return list(m_set['Tag'])


#Function to get all map points for a given start position and time limit.  Includes pre-computed dictionaries 
#for saving time in the long run

#define global variable  for recording pre-compiled times from certain stops
pre_compiled=defaultdict(list)

def get_map_points(route_dictionary,All_Stops_df,transfer_dictionary,add_string,time_limit,distance=100,
                   r_class='wkd'):
    locator=set_locator()
    stat_coord=get_Toronto_coords(add_string,locator)
    initial_stops=near_stops(All_Stops_df,stat_coord,distance)
    initial_stops=[(stop,0) for stop in initial_stops]
    #Had pre_compiled
    return_tups=compute_stops(route_dictionary,All_Stops_df,transfer_dictionary,time_limit,r_class,
                         initial_stops)
    return_tups=get_coords_for_tups(return_tups.copy(),All_Stops_df)
    
    return return_tups, stat_coord
    
    
def create_map(route_dictionary,All_Stops_df,transfer_dictionary,add_string,time_limit,distance=100,r_class='wkd'):
    #had pre_compiled
    route_tups, stat_coord=get_map_points(route_dictionary,All_Stops_df,transfer_dictionary,add_string,time_limit,distance,r_class)
    
    map_toronto = folium.Map(location=[43.653963, -79.387207], zoom_start=14)  
    folium.CircleMarker((stat_coord[0],stat_coord[1])).add_to(map_toronto)
    
    #define time bounds
    colors=['blue','green','yellow','red','purple']
    
    for  tup in route_tups:
        if tup[1]<= time_limit// 5:
            folium.Circle((tup[2],tup[3]), radius=18, weight=3,
                        color=colors[0],fill_color=colors[0],fill_opacity=.5).add_to(map_toronto)
        elif tup[1]>time_limit//5 and tup[1]<=(2*time_limit)//5:
            folium.Circle((tup[2],tup[3]), radius=18, weight=3,
                        color=colors[1],fill_color=colors[1],fill_opacity=.5).add_to(map_toronto)
        elif tup[1]>(2*time_limit)//5 and tup[1]<=(3*time_limit)//5:
            folium.Circle((tup[2],tup[3]), radius=18, weight=3,
                        color=colors[2],fill_color=colors[2],fill_opacity=.5).add_to(map_toronto)
        elif tup[1]>(3*time_limit)//5 and tup[1]<=(4*time_limit)//5:
            folium.Circle((tup[2],tup[3]), radius=18, weight=3,
                        color=colors[3],fill_color=colors[3],fill_opacity=.5).add_to(map_toronto)
        else:
            folium.Circle((tup[2],tup[3]), radius=18, weight=3,
                        color=colors[4],fill_color=colors[4],fill_opacity=.5).add_to(map_toronto)
                    
    map_toronto.save('map_display.html')
    
    


def compute_stops(route_dictionary,All_Stops_df,transfer_dictionary,time_limit,r_class,
                  initial_stops,list_of_stops=[],return_tups=[],time_used=0):
    for stop_tup in initial_stops:
        stop=stop_tup[0]
        used_time=stop_tup[1]
        if stop not in list_of_stops and used_time<time_limit:
           print(stop)

          # if not pre_compiled[stop]:
           list_of_stops.append(stop)
           route=list(All_Stops_df.loc[All_Stops_df['Tag']==stop].route)[0]
           transfer_tups=transfer_dictionary[stop].copy()
           transfer_tups=get_next_stops(stop,route_dictionary,route,All_Stops_df,r_class,transfer_tups.copy())
           transfer_tups=[(tup[0],tup[1],tup[2]+used_time) for tup in transfer_tups]
        
                
           new_starts=[(tup[0],tup[2]) for tup in transfer_tups.copy() if tup[0] not in list_of_stops]
           return_tups=check_min_time(transfer_tups,return_tups.copy(),time_limit)
           append_stops=compute_stops(route_dictionary,All_Stops_df,transfer_dictionary,time_limit,r_class,new_starts,list_of_stops,return_tups,used_time)
           return_tups=check_min_time(append_stops,return_tups.copy(),time_limit)
                
                
                #pre_compiled[stop]=[(tup[0],tup[1]-time_used) for tup in return_tups]
          # else:            
          #      new_starts=pre_compiled[stop]
          #      append_stops=compute_stops(route_dictionary,transfer_dictionary,time_limit,r_class,pre_compiled,
          #      new_starts,list_of_stops,return_tups,used_time)                
               
               # pre_compiled[stop]=check_min_time(pre_compiled[stop],append_stops)
                
          #      return_tups=check_min_time(return_tups,append_stops,time_limit)
        
    return return_tups
            
def check_min_time(transfer_tups,return_tups,time_limit=10000):
    return_tag_list=[tup[0] for tup in return_tups]
    for tup in transfer_tups:
        if tup[0] not in return_tag_list and tup[2]<=time_limit:
            return_tups.append(tup)
        elif tup[0] in return_tag_list and return_tups[return_tag_list.index(tup[0])][2]>tup[2]:
            return_tups[return_tag_list.index(tup[0])]=tup  
    return return_tups                

def get_next_stops(stop,route_dictionary,route,All_Stops_df,r_class,transfer_tups=[]) :
    dir_dict=route_dictionary[route][r_class]
    for direction in dir_dict:
        route_df=route_dictionary[route][r_class][direction]
        if stop in route_df['Tag'] and route_df.loc[route_df['Tag']==stop].index<len(route_df):
            next_stop=route_df.iloc[route_df.loc[route_df['Tag']==stop].index+1]
            transfer_tups.append((next_stop['Tag'],route,next_stop['Time_diff']))
        else:
            ind=All_Stops_df.loc[All_Stops_df['Tag']==stop].index
            sub_search=All_Stops_df.loc[(All_Stops_df['route']==route)]
            sub_search=sub_search.loc[sub_search.index<ind.to_list()[0]]
            sub_search_b=sub_search.loc[sub_search['Tag'].isin(route_df['Tag'])]['Tag'].to_list()
            if len(sub_search_b)>0:
            #Should now have sub_search as df of stops on route in direction before the desired stop
            #Can then interpolate times to next timed stop time after finding nearest stop with a time code
                sub_search_b=sub_search.loc[sub_search['Tag'].isin(route_df['Tag'])]['Tag'].to_list()[-1]
                if route_df.loc[route_df['Tag']==sub_search_b].index.to_list()[0]+1<len(route_df):    
                    next_stop=route_df.iloc[route_df.loc[route_df['Tag']==sub_search_b].index.to_list()[0]+1]
                    transfer_tups.append((next_stop['Tag'],route,next_stop['Time_diff']))   
    return transfer_tups
        
def get_coords_for_tups(return_tups,All_Stops_df):
    return_list=[]
    for tup in return_tups:
        lat=list(All_Stops_df.loc[All_Stops_df['Tag']==tup[0]].Lat.astype(float))[0]
        lon=list(All_Stops_df.loc[All_Stops_df['Tag']==tup[0]].Lon.astype(float))[0]
        return_list.append((tup[0],tup[1],tup[2],lat,lon))
    return  return_list
         
#def pre_compute_routes(route_dictionary,All_Stops_df,transfer_dictionary,r_class,
#        list_of_stops=[],return_tups=[],time_used=0):
    #initialize and pre-compute routes to store in a dictionary for arbitrarily high time_limit
            
        
        
    



    








    

