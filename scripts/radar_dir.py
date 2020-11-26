"""
Turning radar PPIs into Cartesian grids.

@title: radar_grids
@name: radar_grids
@author: Valentin Louf <valentin.louf@bom.gov.au>
@institution: Monash University and the Australian Bureau of Meteorology
@date: 27/11/2020

.. autosummary::
    :toctree: generated/

    chunks
    buffer
    main
"""
# Python Standard Library
import os
import sys
import glob
import time
import argparse
import datetime
import warnings
import traceback

from concurrent.futures import TimeoutError
from pebble import ProcessPool, ProcessExpired

import radar_grids


def chunks(l, n):
    """
    Yield successive n-sized chunks from l.
    From http://stackoverflow.com/a/312464
    """
    for i in range(0, len(l), n):
        yield l[i : i + n]


def buffer(infile: str) -> None:
    """
    It calls the production line and manages it. Buffer function that is used
    to catch any problem with the processing line without screwing the whole
    multiprocessing stuff.

    Parameters:
    ===========
    infile: str
        Name of the input radar file.    
    """
    radar_grids.标准映射(infile, OUTPATH, prefix="twp10cpolgrid", na_standard=True)

    return None


def main() -> None:
    input_dir = os.path.join(INPATH, "**", "*.*")
    flist = sorted(glob.glob(input_dir))
    if len(flist) == 0:
        print(f"No file found for {INPATH}.")
        return None

    for flist_chunk in chunks(flist, 32):
        with ProcessPool() as pool:
            future = pool.map(buffer, flist_chunk, timeout=60)
            iterator = future.result()

            while True:
                try:
                    _ = next(iterator)
                except StopIteration:
                    break
                except TimeoutError as error:
                    print("function took longer than %d seconds" % error.args[1])
                except ProcessExpired as error:
                    print("%s. Exit code: %d" % (error, error.exitcode))
                except Exception:
                    traceback.print_exc()

    return None


if __name__ == "__main__":
    """
    Global variables definition.
    """
    # Parse arguments
    parser_description = "Processing of radar data from level 1a to level 1b."
    parser = argparse.ArgumentParser(description=parser_description)
    parser.add_argument(
        "-i",
        "--input-dir",
        dest="indir",
        default="/g/data/hj10/admin/opol/level_1b/ppi/in2017_c02/",
        type=str,
        help="Input directory.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        dest="outdir",
        default="/g/data/hj10/admin/incoming/opol/gridded/in2017_c02/",
        type=str,
        help="Output directory.",
    )

    args = parser.parse_args()
    INPATH = args.indir
    OUTPATH = args.outdir
    print(f"The input directory is {INPATH}\nThe output directory is {OUTPATH}.")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        main()
