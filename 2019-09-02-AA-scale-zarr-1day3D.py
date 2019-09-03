#!/usr/bin/env python


from dask_jobqueue import SLURMCluster 
from dask.distributed import Client 
  
cluster = SLURMCluster(cores=1,name='make_zarr',walltime='00:30:00',job_extra=['--constraint=HSW24','--exclusive','--nodes=1','--ntasks-per-node=1'],memory='120GB',interface='ib0') 
print(cluster.job_script()) 

cluster.scale(100) 


from dask.distributed import Client
client = Client(cluster)


import xarray as xr
import dask
import zarr
import numpy as np                                                                                        
import os
import time
import glob

nb_workers = 0
while True:
        nb_workers = len(client.scheduler_info()["workers"])
        if nb_workers >= 2:
                break
        time.sleep(1)


compressor = zarr.Blosc(cname='zstd', clevel=3, shuffle=2)                   

date='20100101'
print('zarrifying T ',str(date))
year=date[0:4]
month=date[4:6]
day=date[6:]

if month in ['07','08','09','10']:
	CASE='BLB002'
elif month in ['12','01','02','03','04','05','06']:
	CASE='BLB002X'
elif month == '11':
	if day in ['01','02','03','04','05','06','07']:
		CASE='BLB002'
	else:
		CASE='BLB002X'

datadir='/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-'+str(CASE)+'-S/'
files=sorted(glob.glob(datadir+'*/eNATL60-'+str(CASE)+'_1h_*_gridT_'+year+month+day+'-'+year+month+day+'.nc'))
data=xr.open_mfdataset(files[0],chunks={'time_counter':1,'y':1000,'x':1000,'deptht':1})['votemper']

ds=data.to_dataset(name='votemper')

encoding = {vname: {'compressor': compressor} for vname in ds.variables}
ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/eNATL60/scale/zarr_temp3D_20100101', encoding=encoding)

print('zarr done')

ds=xr.open_mfdataset('/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLB002-S/1h/surf/*'+str(year)+'m'+str(mm)+'*s zocrtx.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':672,'y':120,'x':120})

encoding = {vname: {'compressor': compressor} for vname in ds.variables}

ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/eNATL60/scale/zarr_eNATL60-BLB002-SSU-1h-y'+str(year)+'m'+str(mm), encoding=encoding)
print('ending month ',str(m))



