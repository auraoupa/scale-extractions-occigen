#!/usr/bin/env python

import xarray as xr
import dask
import numpy as np
import os
import time
import glob
from datetime import date
today=date.today()

import sys
sys.path.insert(0,'/home/albert7a/git/xscale')
import xscale

from dask_jobqueue import SLURMCluster 
from dask.distributed import Client 
  
cluster = SLURMCluster(cores=1,name='make_profiles',walltime='00:30:00',job_extra=['--constraint=HSW24','--exclusive','--nodes=1','--ntasks-per-node=24'],memory='120GB',interface='ib0') 
print(cluster.job_script()) 

cluster.scale(240) 

from dask.distributed import Client
client = Client(cluster)


nb_workers = 0
while True:
	nb_workers = len(client.scheduler_info()["workers"])
	if nb_workers >= 2:
		break
	time.sleep(1)


data_dir = '/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-BLBT02-S/'
gridfile='/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-I/mesh_mask_eNATL60_3.6_lev1.nc4'
dsgrid=xr.open_dataset(gridfile,chunks={'x':1000,'y':1000})




def filt(w):
    win_box2D = w.window
    win_box2D.set(window='hanning', cutoff=20, dim=['x', 'y'], n=[30, 30])
    bw = win_box2D.boundary_weights(drop_dims=[])
    w_LS = win_box2D.convolve(weights=bw)
    w_SS=w-w_LS
    return w_SS




def compute_2D_filter(date):
    print('read the data')
    tfilename = sorted(glob.glob(data_dir+'*/eNATL60-BLBT02_1h_*_gridT_'+date+'-'+date+'.nc'))
    tfile=tfilename[0]
    dst=xr.open_dataset(tfile,chunks={'x':1000,'y':1000,'time_counter':1,'deptht':1})
    tdata=dst['votemper']
    print('filter data')
    tprime=filt(tdata)
    output_name='/scratch/cnt0024/hmg2840/albert7a/eNATL60/scale/eNATL60-BLBT02_y'+date[0:4]+'m'+date[4:6]+'d'+date[6:9]+'_spatial_filter_100km.nc'
    if not os.path.exists(output_name):
            print('to dataset')
            dataset=tprime.to_dataset(name='tprime')
            dataset['tprime'].attrs=tdata.attrs
            dataset['tprime'].attrs['standard_name']='small scales temperature' 
            dataset['tprime'].attrs['long_name']='tprime'
            dataset['tprime'].attrs['units']='deg C'        
            dataset.attrs['global_attribute']= 'small scales temperature computed on occigen with dask-jobqueue '+str(today)
            print('to netcdf')
            dataset.to_netcdf(path=output_name,mode='w')




compute_2D_filter('20090701')








