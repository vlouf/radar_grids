"""
Turning radar PPIs into Cartesian grids. Processing the Australian
National archive.

@creator: Valentin Louf <valentin.louf@bom.gov.au>
@institution: Monash University and Bureau of Meteorology
@date: 12/04/2021

.. autosummary::
    :toctree: generated/

    buffer
    check_rid
    extract_zip
    get_radar_archive_file
    mkdir
    remove
    savedata
    main
"""
import os
import sys
import time
import zipfile
import argparse
import warnings
import traceback

from typing import List

import numpy as np
import pandas as pd
import dask.bag as db

import radar_grids


class Chronos():
    def __init__(self, messg=None):
        self.messg = messg
    def __enter__(self):
        self.start = time.time()
    def __exit__(self, ntype, value, traceback):
        self.time = time.time() - self.start
        if self.messg is not None:
            print(f"{self.messg} took {self.time:.2f}s.")
        else:
            print(f"Processed in {self.time:.2f}s.")


def buffer(infile: str, outpath: str, prefix: str) -> None:
    """
    It calls the production line and manages it. Buffer function that is used
    to catch any problem with the processing line without screwing the whole
    multiprocessing stuff.

    Parameters:
    ===========
    infile: str
        Name of the input radar file.
    """
    try:
        radar_grids.标准映射(infile, outpath, prefix=prefix, na_standard=True)
    except Exception:
        print(f"Problem with file {infile}")
        traceback.print_exc()

    return None


def check_rid() -> bool:
    """
    Check if the Radar ID provided exists.
    """
    indir = f"/g/data/rq0/level_1/odim_pvol/{RID:02}"
    return os.path.exists(indir)


def extract_zip(inzip: str, path: str) -> List[str]:
    """
    Extract content of a zipfile inside a given directory.
    Parameters:
    ===========
    inzip: str
        Input zip file.
    path: str
        Output path.
    Returns:
    ========
    namelist: List
        List of files extracted from  the zip.
    """
    with zipfile.ZipFile(inzip) as zid:
        zid.extractall(path=path)
        namelist = [os.path.join(path, f) for f in zid.namelist()]
    return namelist


def get_radar_archive_file(date) -> str:
    """
    Return the archive containing the radar file for a given radar ID and a
    given date.
    Parameters:
    ===========
    date: datetime
        Date.
    Returns:
    ========
    file: str
        Radar archive if it exists at the given date.
    """
    datestr = date.strftime("%Y%m%d")
    file = f"/g/data/rq0/level_1/odim_pvol/{RID:02}/{date.year}/vol/{RID:02}_{datestr}.pvol.zip"
    if not os.path.exists(file):
        return None

    return file


def mkdir(path: str) -> None:
    """
    Create the DIRECTORY(ies), if they do not already exist
    """
    try:
        os.mkdir(path)
    except FileExistsError:
        pass

    return None


def remove(flist: List[str]) -> None:
    """
    Remove file if it exists.
    """
    flist = [f for f in flist if f is not None]
    for f in flist:
        try:
            os.remove(f)
        except FileNotFoundError:
            pass
    return None


def main(date_range) -> None:
    """
    Loop over dates:
    1/ Unzip archives.
    2/ Generate clutter mask for given date.
    3/ Generate composite mask.
    4/ Get the 95th percentile of the clutter reflectivity.
    5/ Save data for the given date.
    6/ Remove unzipped file and go to next iteration.
    Parameters:
    ===========
    date_range: Iter
        List of dates to process
    """
    prefix = f"{RID}"
    for date in date_range:
        # Get zip archive for given radar RID and date.
        zipfile = get_radar_archive_file(date)
        if zipfile is None:
            print(f"No file found for radar {RID} at date {date}.")
            continue

        # Unzip data/
        namelist = extract_zip(zipfile, path=ZIPDIR)
        outpath = os.path.join(OUTPATH, prefix)
        mkdir(outpath)
        argslist = [(f, outpath, prefix) for f in namelist]
        with Chronos(f"Radar {RID} at date {date}"):
            bag = db.from_sequence(argslist).starmap(buffer)
            _ = bag.compute()

        # Removing unzipped files, collecting memory garbage.
        remove(namelist)


    return None


if __name__ == "__main__":
    ZIPDIR: str = "/scratch/kl02/vhl548/unzipdir/"

    parser_description = "Relative Calibration Adjustment (RCA) - Monitoring of clutter radar reflectivity."
    parser = argparse.ArgumentParser(description=parser_description)
    parser.add_argument(
        "-s", "--start-date", dest="start_date", type=str, help="Left bound for generating dates.", required=True
    )
    parser.add_argument(
        "-e", "--end-date", dest="end_date", type=str, help="Right bound for generating dates.", required=True
    )
    parser.add_argument(
        "-r",
        "--rid",
        dest="rid",
        type=int,
        required=True,
        help="The individual radar Rapic ID number for the Australian weather radar network.",
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output",
        default="/scratch/kl02/vhl548/s3car-server/cluttercal/",
        type=str,
        help="Output directory",
    )

    args = parser.parse_args()
    RID = args.rid
    START_DATE = args.start_date
    END_DATE = args.end_date
    OUTPATH = args.output

    try:
        date_range = pd.date_range(START_DATE, END_DATE)
        if len(date_range) == 0:
            parser.error("End date older than start date.")
    except Exception:
        parser.error("Invalid dates.")
        sys.exit()

    mkdir(OUTPATH)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        main(date_range)
