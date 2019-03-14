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
            return None

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
        gridding_algo="map_gates_to_grid", weighting_function='Barnes',
        map_roi=True, toa=20000, copy_field_data=True, algorithm='kd_tree',
        leafsize=10., roi_func='dist_beam', constant_roi=2500,
        z_factor=0.05, xy_factor=0.02, min_radius=500.0,
        h_factor=1.0, nb=1.5, bsp=1.0, skip_transform=False)

    # Removing obsolete fields
    grid_70km.fields.pop('ROI')
    grid_70km.fields.pop('raw_velocity')

    # Change name of reflectivity
    grid_70km.add_field("reflectivity_gridded_dBZ", grid_70km.fields.pop('reflectivity'))
    grid_70km.fields['reflectivity_gridded_dBZ']['comment'] = "DO NOT USE. Please use the reflectivity_gridded_Z as default reflectivity field."

    # Switch linear reflectivity back to dBZ
    grid_70km.fields['reflectivity_gridded_Z']['data'] = 10 * np.log10(grid_70km.fields['reflectivity_gridded_Z']['data'])
    grid_70km.fields['reflectivity_gridded_Z']['comment'] = "Reflectivity field of reference."

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
        gridding_algo="map_gates_to_grid", weighting_function='Barnes',
        map_roi=True, toa=20000, copy_field_data=True, algorithm='kd_tree',
        leafsize=10., roi_func='dist_beam', constant_roi=2500,
        z_factor=0.05, xy_factor=0.02, min_radius=500.0,
        h_factor=1.0, nb=1.5, bsp=1.0, skip_transform=False)


    # Removing obsolete fields
    grid_150km.fields.pop('ROI')
    grid_150km.fields.pop('raw_velocity')

    # Change name of reflectivity
    grid_150km.add_field("reflectivity_gridded_dBZ", grid_150km.fields.pop('reflectivity'))
    grid_150km.fields['reflectivity_gridded_dBZ']['comment'] = "DO NOT USE. Please use the reflectivity_gridded_Z as default reflectivity field."

    # Switch linear reflectivity back to dBZ
    grid_150km.fields['reflectivity_gridded_Z']['data'] = 10 * np.log10(grid_150km.fields['reflectivity_gridded_Z']['data'])
    grid_150km.fields['reflectivity_gridded_Z']['comment'] = "Reflectivity field of reference."

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

    return None


def radar_gridding(infile, output_directory):
    sttime = time.time()
    radar = pyart.io.read(infile)
    radar_start_date = netCDF4.num2date(radar.time['data'][0], radar.time['units'].replace("since", "since "))

    obsolete_keys = ["total_power", ]
    for key in obsolete_keys:
        try:
            radar.fields.pop(key)
        except KeyError:
            continue

    # Linear reflectivity
    refl = radar.fields['reflectivity']['data'].copy()
    linear_z = dict()
    eta = (10 ** (refl / 10)).astype(np.float32)
    np.ma.set_fill_value(eta, np.NaN)
    linear_z['data'] = eta
    linear_z['units'] = 'dBZ'
    linear_z['long_name'] = 'Corrected reflectivity gridded linearly'
    linear_z['units']
    linear_z['_FillValue'] = np.NaN
    linear_z['_Least_significant_digit'] = 4

    radar.add_field('reflectivity_gridded_Z', linear_z)

    outpath_150 = os.path.join(output_directory, "grid_150km_2500m")
    mkdir(outpath_150)
    gridding_radar_150km(radar, radar_start_date, outpath_150)

    outpath_70 = os.path.join(output_directory, "grid_70km_1000m")
    mkdir(outpath_70)
    gridding_radar_70km(radar, radar_start_date, outpath_70)

    print(crayons.green(f"{os.path.basename(infile)} processed in {time.time() - sttime:0.2f}."))

    return None
