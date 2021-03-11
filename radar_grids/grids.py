"""
Gridding radar data using Barnes2 and a constant ROI from Py-ART

@title: grids.py
@author: Valentin Louf <valentin.louf@bom.gov.au>
@institutions: Monash University and the Australian Bureau of Meteorology
@date: 11/03/2021

.. autosummary::
    :toctree: generated/

    mkdir
    get_dtype
    update_metadata
    update_variables_metadata
    grid_radar
    multiple_gridding
"""
# Standard Library
import os
import uuid
import datetime
import traceback
import collections

from typing import Any, Dict, Tuple

# Other libraries.
import pyart
import cftime
import netCDF4
import numpy as np


def mkdir(dirpath: str) -> None:
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


def get_dtype() -> Dict:
    keytypes = {
        "air_echo_classification": np.int16,
        "radar_echo_classification": np.int16,
        "corrected_differential_phase": np.float32,
        "corrected_differential_reflectivity": np.float32,
        "corrected_reflectivity": np.float32,
        "corrected_specific_differential_phase": np.float32,
        "corrected_velocity": np.float32,
        "cross_correlation_ratio": np.float32,
        "normalized_coherent_power": np.float32,
        "radar_estimated_rain_rate": np.float32,
        "signal_to_noise_ratio": np.float32,
        "spectrum_width": np.float32,
        "total_power": np.float32,
    }

    return keytypes


def update_metadata(radar, longitude: np.ndarray, latitude: np.ndarray) -> Dict:
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
        "geospatial_vertical_min": np.int32(radar.origin_altitude["data"][0]),
        "geospatial_vertical_max": np.int32(20000),
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
    outpath: str=None,
    prefix: str="502",
    refl_name: str="corrected_reflectivity",
    grid_shape: Tuple=(41, 117, 117),
    grid_xlim: Tuple=(-150000, 150000),
    grid_ylim: Tuple=(-150000, 150000),
    grid_zlim: Tuple=(0, 20000),
    constant_roi: float=2500,
) -> Any:
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
        # Like the national archive: ID_YYYYMMDD_HHMMSS.nc
        datetimestr = date.strftime("%Y%m%d_%H%M")
        outfilename = f"{prefix}_{datetimestr}00_grid.nc"
        outfilename = os.path.join(outpath, outfilename)
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

    # Update dtype:
    keytypes = get_dtype()
    for k, v in keytypes.items():
        try:
            if grid.fields[k]["data"].dtype != v:
                grid.fields[k]["data"] = grid.fields[k]["data"].astype(v)
        except KeyError:
            pass

    # Metadata
    lon_data, lat_data = grid.get_point_longitude_latitude(0)
    metadata = update_metadata(grid, longitude=lon_data, latitude=lat_data)
    metadata["summary"] = f"Gridded data from radar {prefix}."
    for k, v in metadata.items():
        grid.metadata[k] = v
    grid.metadata["title"] = f"Gridded radar volume on a {2*max(grid_xlim)}x{2*max(grid_ylim)}x{max(grid_zlim)}km grid"
    grid = update_variables_metadata(grid)
    # A-Z order.
    metadata = grid.metadata
    grid.metadata = collections.OrderedDict(sorted(metadata.items()))

    # Saving data.
    if outfilename is not None:
        pyart.io.write_grid(outfilename, grid, arm_time_variables=True, write_point_lon_lat_alt=False)
        # append ROI and lat/long 2D grids and update metadata
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
    prefix: str = "502",
    refl_name: str = "corrected_reflectivity",
    na_standard: bool = False,
) -> None:
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

        fkeys = list(radar.fields.keys())
        if "corrected_reflectivity" not in fkeys or refl_name not in fkeys:
            raise KeyError("Missing important radar field.")

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

    kwargs = {
        "outpath": outpath,
        "refl_name": refl_name,
        "prefix": prefix,
        "grid_shape": (41, 117, 117),
        "grid_xlim": (-150000, 150000),
        "grid_ylim": (-150000, 150000),
        "grid_zlim": (0, 20000),
        "constant_roi": 2500,
    }

    try:
        grid_radar(radar, **kwargs)
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

    kwargs["outpath"] = outpath
    kwargs["grid_shape"] = (41, 301, 301)

    try:
        grid_radar(radar, **kwargs)
    except Exception:
        traceback.print_exc()
        pass

    return None
