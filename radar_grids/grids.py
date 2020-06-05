"""
Gridding radar data using Barnes2 and a constant ROI from Py-ART

@title: grids.py
@author: Valentin Louf <valentin.louf@monash.edu>
@institutions: Monash University and the Australian Bureau of Meteorology
@date: 16/03/2020

.. autosummary::
    :toctree: generated/

    mkdir
    gridding_radar_70km
    gridding_radar_150km
    radar_gridding
"""
# Standard Library
import os
import time
import uuid
import datetime

# Other libraries.
import crayons
import pyart
import netCDF4
import numpy as np


def mkdir(dirpath):
    '''
    Create directory. Check if directory exists and handles error.
    '''
    if not os.path.exists(dirpath):
        # Might seem redundant, but the multiprocessing creates error.
        try:
            os.mkdir(dirpath)
        except FileExistsError:
            pass

    return None


def gridding_radar_70km(radar, radar_date, outpath):
    """
    Map a single radar to a Cartesian grid of 70 km range and 2.5 km resolution.

    Parameters:
    ===========
    radar:
        Py-ART radar structure.
    radar_date: datetime
        Datetime stucture of the radar data.
    outpath: str
        Ouput directory.
    """
    # Extracting year, date, and datetime.
    year = str(radar_date.year)
    datestr = radar_date.strftime("%Y%m%d")
    datetimestr = radar_date.strftime("%Y%m%d.%H%M")
    fname = "twp10cpolgrid70.b2.{}00.nc".format(datetimestr)

    # Output directory
    outdir_70km = os.path.join(outpath, year)
    mkdir(outdir_70km)

    outdir_70km = os.path.join(outdir_70km, datestr)
    mkdir(outdir_70km)

    # Output file name
    outfilename = os.path.join(outdir_70km, fname)
    if os.path.exists(outfilename):
        return None

    # exclude masked gates from the gridding
    my_gatefilter = pyart.filters.GateFilter(radar)
    my_gatefilter.exclude_transition()
    my_gatefilter.exclude_masked('reflectivity')

    # Gridding
    grid_70km = pyart.map.grid_from_radars(
        radar, gatefilters=my_gatefilter,
        grid_shape=(41, 141, 141),
        grid_limits=((0, 20000), (-70000.0, 70000.0), (-70000.0, 70000.0)),
        gridding_algo="map_gates_to_grid", weighting_function='Barnes2', roi_func='constant', constant_roi=1000,)

    # Removing obsolete fields
    grid_70km.fields.pop('ROI')
    try:
        grid_70km.fields.pop('raw_velocity')
    except KeyError:
        pass

    # Metadata
    today = datetime.datetime.utcnow()
    metadata = grid_70km.metadata.copy()
    metadata['history'] = "created by Valentin Louf on raijin.nci.org.au at " + today.isoformat() + " using Py-ART"
    metadata['processing_level'] = 'b2'
    metadata['title'] = "Gridded radar volume on a 70x70x20km grid from CPOL"
    metadata['uuid'] = str(uuid.uuid4())
    metadata['field_names'] = ", ".join([k for k in grid_70km.fields.keys()])

    grid_70km.metadata = metadata

    # Saving data.
    pyart.io.write_grid(outfilename, grid_70km, write_point_lon_lat_alt=True)

    del grid_70km
    return None


def gridding_radar_150km(radar, radar_date, outpath):
    """
    Map a single radar to a Cartesian grid of 150 km range and 2.5 km resolution.

    Parameters:
    ===========
        radar:
            Py-ART radar structure.
        radar_date: datetime
            Datetime stucture of the radar data.
        outpath: str
            Ouput directory.
    """
    # Extracting year, date, and datetime.
    year = str(radar_date.year)
    datestr = radar_date.strftime("%Y%m%d")
    datetimestr = radar_date.strftime("%Y%m%d.%H%M")
    fname = "twp10cpolgrid150.b2.{}00.nc".format(datetimestr)

    # Output directory
    outdir_150km = os.path.join(outpath, year)
    mkdir(outdir_150km)

    outdir_150km = os.path.join(outdir_150km, datestr)
    mkdir(outdir_150km)

    # Output file name
    outfilename = os.path.join(outdir_150km, fname)
    if os.path.exists(outfilename):
        return None

    # exclude masked gates from the gridding
    my_gatefilter = pyart.filters.GateFilter(radar)
    my_gatefilter.exclude_transition()
    my_gatefilter.exclude_masked('reflectivity')

    # Gridding
    grid_150km = pyart.map.grid_from_radars(
        radar, gatefilters=my_gatefilter,
        grid_shape=(41, 117, 117),
        grid_limits=((0, 20000), (-145000.0, 145000.0), (-145000.0, 145000.0)),
        gridding_algo="map_gates_to_grid", weighting_function='Barnes2', roi_func='constant', constant_roi=2500,)

    # Removing obsolete fields
    grid_150km.fields.pop('ROI')
    try:
        grid_150km.fields.pop('raw_velocity')
    except KeyError:
        pass

    # Metadata
    today = datetime.datetime.utcnow()
    metadata = grid_150km.metadata.copy()
    metadata['history'] = "created by Valentin Louf on raijin.nci.org.au at " + today.isoformat() + " using Py-ART"
    metadata['processing_level'] = 'b2'
    metadata['title'] = "Gridded radar volume on a 150x150x20km grid from CPOL"
    metadata['uuid'] = str(uuid.uuid4())
    metadata['field_names'] = ", ".join([k for k in grid_150km.fields.keys()])

    grid_150km.metadata = metadata

    # Saving data.
    pyart.io.write_grid(outfilename, grid_150km, write_point_lon_lat_alt=True)

    del grid_150km
    return None


def gridding(infile, output_directory):
    """
    Call the 2 gridding functions to generate a full domain grid at 2.5 km
    resolution and a half-domain grid at 1 km resolution

    Parameters:
    ===========
    infile: str
        Inpute radar file
    output_directory: str
        Ouput directory.
    """
    sttime = time.time()
    radar = pyart.io.read(infile)
    radar_start_date = netCDF4.num2date(radar.time['data'][0], radar.time['units'].replace("since", "since "))

    obsolete_keys = ["total_power", ]
    for key in obsolete_keys:
        try:
            radar.fields.pop(key)
        except KeyError:
            continue

    if "reflectivity" not in radar.fields.keys():
        if "corrected_reflectivity" in radar.fields.keys():
            radar.add_field("reflectivity", radar.fields.pop("corrected_reflectivity"))

    outpath_150 = os.path.join(output_directory, "grid_150km_2500m")
    mkdir(outpath_150)
    gridding_radar_150km(radar, radar_start_date, outpath_150)

    outpath_70 = os.path.join(output_directory, "grid_70km_1000m")
    mkdir(outpath_70)
    gridding_radar_70km(radar, radar_start_date, outpath_70)

    print(crayons.green(f"{os.path.basename(infile)} processed in {time.time() - sttime:0.2f}."))

    del radar
    return None
