#!/usr/bin/env python

import sys
import os
import glob

import argparse
from multiprocessing import Pool
import tqdm
import tables
import pandas as pd
import numpy as np

def recast_uint(df):
    for column, dtype in zip(df.columns, df.dtypes):
        if(dtype == np.uint16):
            df[column] = df[column].astype(np.int16)
        elif(dtype == np.uint32):
            df[column] = df[column].astype(np.int32)
        elif(dtype == np.uint64):
            df[column] = df[column].astype(np.int64)

def get_dataframe_from_hdf_table(file_object, table_path, column_list=None):
    table_object = file_object.get_node(table_path)
    columns = pd.DataFrame.from_records(table_object.read(0,0))
    if column_list is None:
        column_list = columns.columns.tolist()
        table_dat = table_object.read()
        hdf_dict = {col: table_dat[col] for col in column_list}
    else:
        hdf_dict = {col: table_object.read(field=col) for col in column_list}
    dataframe = pd.DataFrame.from_records(hdf_dict)

    return dataframe

def get_matchfile_metadata(file_object):
    node = file_object.get_node("/matches")
    attribute_names = node._v_attrs._v_attrnamesuser
    return {k: node._v_attrs[k] for k in attribute_names}

def make_positional_dataframe(data_df):

    return data_df.groupby("matchid")[('ra', 'dec', 'matchid')].agg(
                {"ra": "mean", "dec": "mean", "matchid": "count"}).rename(columns={"matchid": "nrec"}).reset_index()


def quad_ccd_to_rc(quadrant, ccd):
    b = 4 * (ccd - 1)
    rc = b + quadrant - 1
    return rc

def assign_matchids_filters(data_df, metadata, transients=False):
    """Assigns globally unique ids to the column "matchid", modifying data_df in place."""
    type_str = "0" if transients else "1"

    data_df.rename(columns={"matchid": "localMatchID"}, inplace=True)
    readout_channel = quad_ccd_to_rc(metadata['quadrantID'], metadata['ccdID'])
    matchid_prefix_string = "{:04d}{:02d}{:01d}".format(metadata['fieldID'],
                                                        readout_channel,
                                                        metadata['filterID'])
    source_prefix_int = int(type_str + matchid_prefix_string +  "000000")
    data_df['matchid'] = (source_prefix_int + data_df['localMatchID']).astype(np.int64)
    data_df['filterid'] = metadata['filterID']

def zone_func(dec):
    zone_height = 10/60.0
    return np.floor((dec + 90.0)/zone_height).astype(int)

def zone_dupe_function(dec):
    zone_height = 20/3600.0
    dupe_height = 5/3600.0

    zone_float = (dec + 90.0)/zone_height
    zone = np.floor(zone_float).astype(int)
    zone_residual = zone_float - zone
    zone_dupe_height = (dupe_height/zone_height)
    delta = 0 + -1*(zone_residual < zone_dupe_height)
    delta += 1*(1 - zone_residual < zone_dupe_height)
    return zone + delta

def convert_matchfile(matchfile_filename, pos_parquet_filename,
                      data_parquet_filename, include_transients=False):

    matchfile_hdf = tables.open_file(matchfile_filename)

    if data_parquet_filename is None:
        column_list = ['matchid', 'ra', 'dec']
    else:
        column_list = None

    sourcedata = get_dataframe_from_hdf_table(matchfile_hdf,
                                              "/matches/sourcedata",
                                              column_list=column_list)

    matchfile_md = get_matchfile_metadata(matchfile_hdf)

    recast_uint(sourcedata)

    assign_matchids_filters(sourcedata, matchfile_md, transients=False)

    source_pos_catalog = make_positional_dataframe(sourcedata)

    if include_transients:
        transientdata = get_dataframe_from_hdf_table(matchfile_hdf,
                                                     "/matches/transientdata",
                                                     column_list=column_list)
        recast_uint(transientdata)
        assign_matchids_filters(transientdata, matchfile_md, transients=True)
        transient_pos_catalog = make_positional_dataframe(transientdata)

        combined_pos_catalog = pd.concat([source_pos_catalog,
                                          transient_pos_catalog])
    else:
        combined_pos_catalog = source_pos_catalog

    combined_pos_catalog['zone'] = zone_func(combined_pos_catalog['dec'])
    combined_pos_catalog['alt_zone'] = zone_dupe_function(combined_pos_catalog['dec'])

    duplicate_records = combined_pos_catalog[combined_pos_catalog['zone'] !=
                                             combined_pos_catalog['alt_zone']].copy()
    duplicate_records['zone'] = duplicate_records['alt_zone']

    duplicated_pos_catalog = pd.concat([combined_pos_catalog,
                                        duplicate_records]).drop(columns=["alt_zone"])

    if not os.path.exists(os.path.dirname(pos_parquet_filename)):
        os.makedirs(os.path.dirname(pos_parquet_filename))

    if pos_parquet_filename is not None:
        duplicated_pos_catalog.to_parquet(pos_parquet_filename)

    if data_parquet_filename is not None:
        if include_transients:
            combined_data = pd.concat([sourcedata, transientdata])
        else:
            combined_data = sourcedata
        combined_data.to_parquet(data_parquet_filename)

    matchfile_hdf.close()

if __name__ == '__main__':

    default_input_basepath = ("/data/epyc/data/ztf_matchfiles/"
                          "partnership/ztfweb.ipac.caltech.edu")
    default_output_basepath = "/data/epyc/data/ztf_scratch/matchfiles_parquet"
    default_glob_pattern = "rc[012]?/*/*.pytable"

    parser = argparse.ArgumentParser(description="Convert hdf5 matchfiles into parquet",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--no-data", dest="no_data", action="store_true",
                        help="Suppress saving the output photometry files, only store positions")
    parser.add_argument("--glob", dest="glob_pattern", action="store",
                        help="Glob pattern for searching the input directory",
                        type=str, default=default_glob_pattern)
    parser.add_argument("--input-path", dest="input_basepath", action="store",
                        help="Input directory",
                        type=str, default=default_input_basepath)
    parser.add_argument("--output-path", dest="output_basepath", action="store",
                        help="Output directory",
                        type=str, default=default_output_basepath)
    parser.add_argument("--nprocs", type=int, default=1,
                        help="Number of parallel processes to use")
    args = parser.parse_args()

    input_files = glob.glob(os.path.join(args.input_basepath, args.glob_pattern))

    # This is not great coding style...
    def process_wrapper(matchfile_path):
        output_file_pytable = matchfile_path.replace(args.input_basepath,
                                                     args.output_basepath)
        output_pos_filename = output_file_pytable.replace(".pytable", "_pos.parquet")
        if args.no_data:
            output_data_filename = None
            if(os.path.exists(output_pos_filename)):
                return
        else:
            output_data_filename = output_file_pytable.replace(".pytable", "_data.parquet")
            if(os.path.exists(output_data_filename)):
                return

        convert_matchfile(matchfile_path, output_pos_filename,
                          output_data_filename)

    if args.nprocs > 1:
        with Pool(args.nprocs) as p:
            p.map(process_wrapper, input_files)
    else:
        for filename in input_files:
            process_wrapper(filename)

