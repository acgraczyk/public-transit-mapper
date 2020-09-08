# -*- coding: utf-8 -*-
"""
Created on Thu Sep  3 11:59:31 2020

@author: Andrew
"""

import math
import numpy as np
import pandas as pd
import API_Backend
import geopandas
from geopy.geocoders import Nominatim
import requests
from flask import Flask, render_template, request, redirect
import re
import folium
from collections import defaultdict


app = Flask(__name__)
columns=['Tag','Name','Lat','Lon','route']
route_dictionary=np.load('route_dict_ttc.npy',allow_pickle=True).item()
All_Stops_df=pd.DataFrame(np.load('all_stops_ttc.npy',allow_pickle=True),columns=columns)
transfer_dictionary=np.load('transfer_dict_ttc.npy',allow_pickle=True).item()
pre_compiled=defaultdict(list)

app.vars={}

@app.route('/', methods=['GET','POST'])
def intro():
  if request.method=='GET':
    
    return render_template('intro_a.html',ans1='Weekday', ans2='Saturday',ans3='Sunday',ans4='Holiday')
    
  else:
   
   app.vars['address']=request.form['address']
   app.vars['time']=request.form['time']
   if request.form['class']=='Weekday':
     app.vars['class']='wkd'
   elif request.form['class']=='Saturday':
     app.vars['class']='sat'
   elif request.form['class']=='Sunday':
     app.vars['class']='sun'
   else:
     app.vars['class']='SIMDAY'

   
   
   fig = create_graph()

   return fig._repr_html_()
  # return page.render(resources=CDN.render(), sym=app.vars['sym'], dat=app.vars['dat'])

def create_graph():
    add_string=app.vars['address']
    time_limit=float(app.vars['time'])
    r_class=app.vars['class']
    locator=API_Backend.set_locator()
    distance=150
    list_of_stops=[]
    stat_coord=API_Backend.get_Toronto_coords(add_string,locator)
    initial_stops=API_Backend.near_stops(All_Stops_df,stat_coord,distance)
    initial_stops=[(stop,0) for stop in initial_stops]
    
    return_tups=API_Backend.compute_stops(route_dictionary,All_Stops_df,transfer_dictionary,time_limit,r_class,
                         initial_stops,list_of_stops)
    route_tups=API_Backend.get_coords_for_tups(return_tups.copy(),All_Stops_df)

    
    map_toronto = folium.Map(location=[43.653963, -79.387207], zoom_start=14)  
    folium.CircleMarker((stat_coord[0],stat_coord[1])).add_to(map_toronto)
    
    #define time bounds
    colors=['blue','green','yellow','red','purple']
    
    for  tup in route_tups:
        tooltip=tup[0]
        on_click='Stop: {}, Route: {} \n Time from address: {} minutes'.format(tup[0],tup[1],round(tup[2]))
        if tup[2]<= time_limit// 5:
            folium.Circle((tup[3],tup[4]), radius=18, weight=3,tooltip=tooltip,popup=on_click,
                        color=colors[0],fill_color=colors[0],fill_opacity=.5).add_to(map_toronto)
        elif tup[2]>time_limit//5 and tup[2]<=(2*time_limit)//5:
            folium.Circle((tup[3],tup[4]), radius=18, weight=3,tooltip=tooltip,popup=on_click,
                        color=colors[1],fill_color=colors[1],fill_opacity=.5).add_to(map_toronto)
        elif tup[2]>(2*time_limit)//5 and tup[2]<=(3*time_limit)//5:
            folium.Circle((tup[3],tup[4]), radius=18, weight=3,tooltip=tooltip,popup=on_click,
                        color=colors[2],fill_color=colors[2],fill_opacity=.5).add_to(map_toronto)
        elif tup[2]>(3*time_limit)//5 and tup[2]<=(4*time_limit)//5:
            folium.Circle((tup[3],tup[4]), radius=18, weight=3,tooltip=tooltip,popup=on_click,
                        color=colors[3],fill_color=colors[3],fill_opacity=.5).add_to(map_toronto)
        else:
            folium.Circle((tup[3],tup[4]), radius=18, weight=3,tooltip=tooltip,popup=on_click,
                        color=colors[4],fill_color=colors[4],fill_opacity=.5).add_to(map_toronto)
                    
    return map_toronto



@app.errorhandler(500)
def error_500(error):
  return render_template('error_handle.html')

@app.errorhandler(404)
def error_400(error):
  return render_template('error_handle.html')

@app.errorhandler(400)
def error_404(error):
  return render_template('error_handle.html')
  

if __name__ == '__main__':
  app.run(port=33507)

