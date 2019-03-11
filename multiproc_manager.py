"""
CPOL Level 1b main production line.

@title: CPOL_PROD_1b
@author: Valentin Louf <valentin.louf@monash.edu>
@institution: Bureau of Meteorology
@date: 1/03/2019
@version: 1

.. autosummary::
    :toctree: generated/

    timeout_handler
    chunks
    production_line_manager
    production_line_multiproc
    main
"""
# Python Standard Library
import os
import sys
import glob
import argparse
import datetime
import traceback

from concurrent.futures import TimeoutError
from pebble import ProcessPool, ProcessExpired


def chunks(l, n):
    """
    Yield successive n-sized chunks from l.
    From http://stackoverflow.com/a/312464
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def main(inargs):
    """
    It calls the production line and manages it. Buffer function that is used
    to catch any problem with the processing line without screwing the whole
    multiprocessing stuff.

    Parameters:
    ===========
    infile: str
        Name of the input radar file.
    outpath: str
        Path for saving output data.
    """
    import warnings
    import traceback

    infile, outpath = inargs

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        import grids

    try:
        grids.radar_gridding(infile, outpath)
    except Exception:
        traceback.print_exc()
        return None

    return None


if __name__ == '__main__':
    """
    Global variables definition.
    """
    # Main global variables (Path directories).
    
    OUTPATH = "/g/data/hj10/cpol_level_1b/v2009/gridded/"    

    # Parse arguments
    parser_description = "Processing of radar data from level 1a to level 1b."
    parser = argparse.ArgumentParser(description=parser_description)
    parser.add_argument(
        '-s',
        '--start-date',
        dest='start_date',
        default=None,
        type=str,
        help='Starting date.',
        required=True)
    parser.add_argument(
        '-e',
        '--end-date',
        dest='end_date',
        default=None,
        type=str,
        help='Ending date.',
        required=True)
    parser.add_argument(
        '-i',
        '--input-dir',
        dest='indir',
        default="/g/data/hj10/cpol_level_1b/v2009/ppi/",
        type=str,
        help='Input directory.')
    parser.add_argument(
        '-o',
        '--output-dir',
        dest='outdir',
        default="/g/data/hj10/cpol_level_1b/v2009/gridded/",
        type=str,
        help='Output directory.')

    args = parser.parse_args()
    START_DATE = args.start_date
    END_DATE = args.end_date
    INPATH = args.indir
    OUTPATH = args.outdir
    try:
        start = datetime.datetime.strptime(START_DATE, "%Y%m%d")
        end = datetime.datetime.strptime(END_DATE, "%Y%m%d")
        if start > end:
            raise ValueError('End date older than start date.')
        date_range = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days + 1, )]
    except ValueError:
        print("Invalid dates.")
        sys.exit()

    try:
        if not os.path.isdir(OUTPATH):
            os.mkdir(OUTPATH)
    except FileExistsError:
        # Extra layer of security due to potential concurrency issue 
        pass

    for day in date_range:
        input_dir = os.path.join(INPATH, str(day.year), day.strftime("%Y%m%d"), "*.*")
        flist = sorted(glob.glob(input_dir))
        if len(flist) == 0:
            print('No file found for {}.'.format(day.strftime("%Y-%b-%d")))
            continue
        print(f'{len(flist)} files found for ' + day.strftime("%Y-%b-%d"))
        for flist_chunk in chunks(flist, 16):
            arglist = [(f, OUTPATH) for f in flist_chunk]
            with ProcessPool() as pool:
                future = pool.map(main, arglist, timeout=60)
                iterator = future.result()
                while True:
                    try:
                        result = next(iterator)
                    except StopIteration:
                        break
                    except TimeoutError as error:
                        print("function took longer than %d seconds" % error.args[1])
                    except ProcessExpired as error:
                        print("%s. Exit code: %d" % (error, error.exitcode))
                    except Exception as error:
                        print("function raised %s" % error)
                        print(error.traceback)  # Python's traceback of remote process

