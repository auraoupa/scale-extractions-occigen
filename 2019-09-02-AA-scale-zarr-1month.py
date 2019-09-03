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

nb_workers = 0
while True:
        nb_workers = len(client.scheduler_info()["workers"])
        if nb_workers >= 2:
                break
        time.sleep(1)


compressor = zarr.Blosc(cname='zstd', clevel=3, shuffle=2)                   

m=7
print('beginning month ',str(m))
mm=str(m).zfill(2)

if m > 6:
    year=2009
else:
    year=2010

ds=xr.open_mfdataset('/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLB002-S/1h/surf/*'+str(year)+'m'+str(mm)+'*sozocrtx.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':672,'y':120,'x':120})

encoding = {vname: {'compressor': compressor} for vname in ds.variables}

ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/eNATL60/scale/zarr_eNATL60-BLB002-SSU-1h-y'+str(year)+'m'+str(mm), encoding=encoding)
print('ending month ',str(m))



