"""
Gridding radar data using Barnes2 and a constant ROI from Py-ART

@title: grids.py
@author: Valentin Louf <valentin.louf@bom.gov.au>
@institutions: Monash University and the Australian Bureau of Meteorology
@date: 27/11/2020

.. autosummary::
    :toctree: generated/

    mkdir
    update_metadata
    update_variables_metadata
    grid_radar
    multiple_gridding
"""
# Standard Library
import os
import time
import uuid
import datetime
import traceback

# Other libraries.
import pyart
import cftime
import netCDF4
import numpy as np


def mkdir(dirpath: str):
    """
    Create directory. Check if directory exists and handles error.
    """
    if not os.path.exists(dirpath):
        # Might seem redundant, but the multiprocessing creates error.
        try:
            os.mkdir(dirpath)
        except FileExistsError:
            pass

    return None


def update_metadata(radar) -> dict:
    """
    Update metadata of the gridded products.

    Parameter:
    ==========
    radar: pyart.core.Grid
        Radar data.

    Returns:
    ========
    metadata: dict
        Output metadata dictionnary.
    """
    today = datetime.datetime.utcnow()
    dtime = cftime.num2pydate(radar.time["data"], radar.time["units"])

    longitude, latitude = radar.get_point_longitude_latitude(0)    
    maxlon = longitude.max()
    minlon = longitude.min()
    maxlat = latitude.max()
    minlat = latitude.min()    

    metadata = {
        "comment": "Gridded radar volume using Barnes et al. ROI",
        "field_names": ", ".join([k for k in radar.fields.keys()]),
        "geospatial_bounds": f"POLYGON(({minlon:0.6} {minlat:0.6},{minlon:0.6} {maxlat:0.6},{maxlon:0.6} {maxlat:0.6},{maxlon:0.6} {minlat:0.6},{minlon:0.6} {minlat:0.6}))",
        "geospatial_lat_max": f"{maxlat:0.6}",
        "geospatial_lat_min": f"{minlat:0.6}",
        "geospatial_lat_units": "degrees_north",
        "geospatial_lon_max": f"{maxlon:0.6}",
        "geospatial_lon_min": f"{minlon:0.6}",
        "geospatial_lon_units": "degrees_east",
        "geospatial_vertical_min": radar.origin_altitude["data"][0],
        "geospatial_vertical_max": 20000,
        "geospatial_vertical_positive": "up",
        "history": f"created by Valentin Louf on gadi.nci.org.au at {today.isoformat()} using Py-ART",
        "processing_level": "b2",
        "time_coverage_start": dtime[0].isoformat(),
        "time_coverage_end": dtime[-1].isoformat(),
        "uuid": str(uuid.uuid4()),
    }

    return metadata


def update_variables_metadata(grid):
    """
    Update metadata of the gridded variables.

    Parameter:
    ==========
    grid: pyart.core.Grid
        Gridded radar data.

    Returns:
    ========
    grid: pyart.core.Grid
        Gridded radar data with updated variables metadata.
    """
    try:
        grid.fields["corrected_velocity"]["standard_name"] = "radial_velocity_of_scatterers_away_from_instrument"
    except KeyError:
        pass

    grid.radar_latitude["standard_name"] = "latitude"
    grid.radar_latitude["coverage_content_type"] = "coordinate"
    grid.radar_longitude["standard_name"] = "longitude"
    grid.radar_longitude["coverage_content_type"] = "coordinate"
    grid.radar_altitude["standard_name"] = "altitude"
    grid.radar_altitude["coverage_content_type"] = "coordinate"
    grid.radar_time["standard_name"] = "time"
    grid.radar_time["coverage_content_type"] = "coordinate"

    grid.point_latitude["standard_name"] = "latitude"
    grid.point_latitude["coverage_content_type"] = "coordinate"
    grid.point_longitude["standard_name"] = "longitude"
    grid.point_longitude["coverage_content_type"] = "coordinate"
    grid.point_altitude["standard_name"] = "altitude"
    grid.point_altitude["coverage_content_type"] = "coordinate"

    return grid


def grid_radar(
    radar,
    infile=None,
    outpath=None,
    prefix="rvopolgrid",
    refl_name="corrected_reflectivity",
    grid_shape=(41, 117, 117),
    grid_xlim=(-150000, 150000),
    grid_ylim=(-150000, 150000),
    grid_zlim=(0, 20000),
    constant_roi=2500,
    na_standard=False,
):
    """
    Map a single radar to a Cartesian grid.

    Parameters:
    ===========
    radar:
        Py-ART radar structure.
    infile:
        Input file name for ouput file name generation.
    outpath: str
        If outpath is not define, it will return the grid and not save it. If
        it is defined, then it will save the grid and return nothing.
    grid_shape: tuple
        Grid shape
    grid_xlim: tuple
        Grid limits in the x-axis.
    grid_ylim: tuple
        Grid limits in the y-axis.
    grid_zlim: tuple
        Grid limits in the z-axis.
    constant_roi: float
        Value for the size of the radius of influence.
    na_standard: bool
        Use the National Archive standard for file-naming convention.

    Returns:
    ========
    grid: pyart.core.Grid
        If the outpath has been set to None, then it will return the grid,
        otherwise it just saves it and return nothing.
    """
    # Update radar dtype:
    radar.altitude["data"] = radar.altitude["data"].astype(np.float64)
    radar.longitude["data"] = radar.longitude["data"].astype(np.float64)
    radar.latitude["data"] = radar.latitude["data"].astype(np.float64)

    date = cftime.num2pydate(radar.time["data"][0], radar.time["units"])
    # Generate filename.
    if outpath is not None:
        if na_standard:
            # Like the national archive: ID_YYYYMMDD_HHMMSS.nc
            datetimestr = date.strftime("%Y%m%d_%H%M")
            outfilename = f"502_{datetimestr}00.nc"
        else:
            datetimestr = date.strftime("%Y%m%d.%H%M")
            if "PPIVol" in infile:
                # Like input file name but PPIVol replaced by GRID
                outfilename = os.path.basename(infile).replace("PPIVol", "GRID")
            else:
                # New name, ARM-style: prefix.b2.date.time.nc
                outfilename = f"{prefix}.b2.{datetimestr}00.nc"
        outfilename = os.path.join(outpath, outfilename)
    else:
        outfilename = None

    if outfilename is not None:
        if os.path.exists(outfilename):
            print(f"Output file {outfilename} already exists. Doing nothing.")
            return None

    # exclude masked gates from the gridding
    gatefilter = pyart.filters.GateFilter(radar)
    gatefilter.exclude_transition()
    gatefilter.exclude_masked(refl_name)

    # Gridding
    grid = pyart.map.grid_from_radars(
        radar,
        gatefilters=gatefilter,
        grid_shape=grid_shape,
        grid_limits=(grid_zlim, grid_xlim, grid_ylim),
        gridding_algo="map_gates_to_grid",
        weighting_function="Barnes2",
        roi_func="constant",
        constant_roi=constant_roi,
    )

    # Removing obsolete fields
    grid.fields.pop("ROI")
    try:
        grid.fields.pop("raw_velocity")
    except KeyError:
        pass

    # Metadata
    metadata = update_metadata(grid)
    for k, v in metadata.items():
        grid.metadata[k] = v
    grid.metadata["title"] = f"Gridded radar volume on a {max(grid_xlim)}x{max(grid_ylim)}x{max(grid_zlim)}km grid"
    grid = update_variables_metadata(grid)

    # Saving data.
    if outfilename is not None:
        pyart.io.write_grid(outfilename, grid, arm_time_variables=True, write_point_lon_lat_alt=False)
        # append ROI and lat/long 2D grids and update metadata
        lon_data, lat_data = grid.get_point_longitude_latitude(0)
        with netCDF4.Dataset(outfilename, "a") as ncid:
            nclon = ncid.createVariable("longitude", np.float32, ("y", "x"), zlib=True, least_significant_digit=2)
            nclon[:] = lon_data
            nclon.units = "degrees_east"
            nclon.standard_name = "longitude"
            nclon.long_name = "longitude_degrees_east"
            nclat = ncid.createVariable("latitude", np.float32, ("y", "x"), zlib=True, least_significant_digit=2)
            nclat[:] = lat_data
            nclat.units = "degrees_north"
            nclat.standard_name = "latitude"
            nclat.long_name = "latitude_degrees_north"

        del grid
        return None
    else:
        return grid


def 标准映射(
    infile: str,
    output_directory: str,
    prefix: str = "rvopolgrid",
    refl_name: str = "corrected_reflectivity",
    na_standard: bool = False,
):
    """
    Call the 2 gridding functions to generate a full domain grid at 2.5 km
    resolution and at 1 km resolution, handle the directory creation.

    Parameters:
    ===========
    infile: str
        Inpute radar file
    output_directory: str
        Ouput directory.
    na_standard: bool
        Use the National Archive standard for file-naming convention.
    """
    try:
        if infile.lower().endswith(("h5", "hdf")):
            radar = pyart.aux_io.read_odim_h5(infile, file_field_names=True)
        else:
            radar = pyart.io.read(infile)
    except Exception:
        print(f"Error while trying to read input file {infile}. Doing nothing.")
        traceback.print_exc()
        return None

    # Drop keys not present in good_keys
    if na_standard:
        good_keys = [
            "air_echo_classification",
            "corrected_differential_phase",
            "corrected_differential_reflectivity",
            "corrected_reflectivity",
            "corrected_specific_differential_phase",
            "corrected_velocity",
            "cross_correlation_ratio",
            "normalized_coherent_power",
            "radar_echo_classification",
            "radar_estimated_rain_rate",
            "reflectivity",
            "signal_to_noise_ratio",
            "spectrum_width",
            "total_power",
        ]
        if refl_name not in good_keys:
            good_keys.append(refl_name)

        fkeys = list(radar.fields.keys())
        for k in fkeys:
            if k not in good_keys:
                try:
                    _ = radar.fields.pop(k)
                except KeyError:
                    pass

    try:
        radar.fields["reflectivity"]
    except KeyError:
        radar.add_field("reflectivity", radar.fields.pop("corrected_reflectivity"))
        refl_name = "reflectivity"

    radar_date = cftime.num2pydate(radar.time["data"][0], radar.time["units"])
    year = str(radar_date.year)
    datestr = radar_date.strftime("%Y%m%d")
    # 150 km 2500m resolution
    outpath = os.path.join(output_directory, "grid_150km_2500m")
    mkdir(outpath)
    outpath = os.path.join(outpath, year)
    mkdir(outpath)
    outpath = os.path.join(outpath, datestr)
    mkdir(outpath)

    try:
        grid_radar(
            radar,
            infile=infile,
            outpath=outpath,
            refl_name=refl_name,
            prefix=prefix,
            grid_shape=(41, 117, 117),
            grid_xlim=(-150000, 150000),
            grid_ylim=(-150000, 150000),
            grid_zlim=(0, 20000),
            constant_roi=2500,
            na_standard=na_standard,
        )
    except Exception:
        traceback.print_exc()
        pass

    # 150 km 1000m resolution
    outpath = os.path.join(output_directory, "grid_150km_1000m")
    mkdir(outpath)
    outpath = os.path.join(outpath, year)
    mkdir(outpath)
    outpath = os.path.join(outpath, datestr)
    mkdir(outpath)

    try:
        grid_radar(
            radar,
            infile=infile,
            outpath=outpath,
            refl_name=refl_name,
            prefix=prefix,
            grid_shape=(41, 301, 301),
            grid_xlim=(-150000, 150000),
            grid_ylim=(-150000, 150000),
            grid_zlim=(0, 20000),
            constant_roi=2500,
            na_standard=na_standard,
        )
    except Exception:
        traceback.print_exc()
        pass

    # 70 km 1000m resolution
    outpath = os.path.join(output_directory, "grid_70km_1000m")
    mkdir(outpath)
    outpath = os.path.join(outpath, year)
    mkdir(outpath)
    outpath = os.path.join(outpath, datestr)
    mkdir(outpath)

    try:
        grid_radar(
            radar,
            infile=infile,
            outpath=outpath,
            refl_name=refl_name,
            prefix=prefix,
            grid_shape=(41, 141, 141),
            grid_xlim=(-70000, 70000),
            grid_ylim=(-70000, 70000),
            grid_zlim=(0, 20000),
            constant_roi=1000,
        )
    except Exception:
        traceback.print_exc()
        pass

    del radar
    return None
