#!/usr/bin/env python3
''' A script to extract only a subset of the full cell directory.
    You can extract by region, municipality or by giving a list of antenas.'''

import argparse
import sys
import pandas as pd
import numpy as np
import s3fs

fs = s3fs.S3FileSystem(anon=False)


def expand_arguments(municipalities=None, regions=None):
    """Return a set of arguments for names and extraction data.

       name_string is to be used for naming output files.
       zone_data contains the zones to use.
       column_data contains either 'Region' or 'Municipality'
       full is used in case no values were given."""
    arg_count = 0
    for option in [municipalities, regions]:
        if (option is not None):
            arg_count += 1

    if arg_count == 0:
        full = True
    else:
        full = False

    zone_data = []
    name_string = ''
    column_data = []

    if full:
        name_string = ''
    else:
        if municipalities is not None:
            column_data.append('Municipal_Code')
            zone_data.append(municipalities.split(','))
            name_string += 'm-' + municipalities + '_'

        if regions is not None:
            column_data.append('Region_Code')
            zone_data.append(regions.split(','))
            name_string += 'r-' + regions + '_'

    return [name_string, zone_data, column_data, full]


def extract_zone(cell_dataframe, column, area):
    """Return dataframe with subset of 'Region' or 'Municipality.'"""
    local_dataframe = cell_dataframe[cell_dataframe[column] == area]
    return local_dataframe


# def extract_antennas(cell_dataframe, antenna_list):
#     """Return dataframe with subset by antenna (probably useless)."""
#     local_df = pd.DataFrame()
#     for antenna in antenna_list:
#         local_df = local_df.append(cell_dataframe[cell_dataframe['Cell_Code'] == antenna])
#    return local_df


def make_zone_df(cell_dataframe, zone_data, column_data, full):
    zone_df = pd.DataFrame()
    if full:
        zone_df = cell_dataframe
    else:
        for i in range(len(zone_data)):
            zone_list = zone_data[i]
            for zone in zone_list:
                local_df = extract_zone(cell_dataframe, column_data[i], int(zone))
                zone_df = zone_df.append(local_df)
    return zone_df


def extract_cell_df(cell_dataframe, municipalities=None, regions=None):
    """Return cell dataframe with only a subset of the antennas.

       The subset can be created with a municipality or region list. If none
       is given, the full list is returned (cleaned). The inputs for the
       options must be strings."""
    name_string, zone_data, column_data, full = expand_arguments(municipalities, regions)

    zone_df = make_zone_df(cell_dataframe, zone_data, column_data, full)

    if zone_df.empty:
        print("ERROR: Your selection criteria led to an empty cell list. Exiting.")
        sys.exit(1)
    else:
        # Remove the CID_X *columns* which are empty
        zone_df = zone_df.reindex(columns=list(zone_df.columns[zone_df.apply(np.max) != -1]))

        # Delete rows with empty CID to avoid wasted computations
        zone_df.drop(zone_df.index[zone_df.loc[:, 'CID_1':].apply(max, axis=1) < 0], inplace=True)
        zone_df.reset_index(drop=True, inplace=True)

        # GSM and UMTS use the LAC+CID strings concatenated. LTE uses something
        # different so we have to separate both
        lte_mask = zone_df.Technology.str.contains('LTE')
        lte_zone_df = zone_df[lte_mask].copy()

        # Keep just the 2G and 3G stuff
        zone_df = zone_df[~lte_mask].copy()

        # Make a front section to concatenate after each operation on the CID sections
        front = zone_df.loc[:, :'Longitude']

        # Make "sum" of LAC and CID for easier comparison with CDR_DF
        column_mask = zone_df.columns.str.contains('CID')
        updated_cid = zone_df.loc[:, zone_df.columns[column_mask]].add(zone_df.LAC * 100000, axis=0)
        zone_df = pd.concat([front, updated_cid], axis=1)
        # Maybe there's a smarter way to do it, but for now I concatenate the first part
        # of an unchanged zone with the updated_cid from above

        # Mask of CID to reset to -1
        reset_mask = (zone_df.loc[:, column_mask].T > zone_df.LAC * 100000).T
        zone_cid = zone_df.loc[:, column_mask][reset_mask].apply(pd.to_numeric).fillna(-1).astype(int)
        zone_df = pd.concat([front, zone_cid], axis=1)

        # Now we do the 4G stuff
        ccaa_codes = {'MAD': 0, 'CAT': 1, 'AND': 2, 'ARA': 3, 'AST': 4,
                      'BAL': 5, 'CAN': 6, 'CTB': 7, 'CLM': 8, 'CYL': 9,
                      'CYM': 10, 'EXT': 11, 'GAL': 12, 'RIO': 13, 'MUR': 14,
                      'NAV': 15, 'PVA': 16, 'VAL': 17}

        offset_codes = {'X': 0, 'Y': 1, 'Z': 2}

        # We modify directly the LAC to then concatenate both dataframes
        lte_zone_df['LAC'] = lte_zone_df.apply(lambda row: ccaa_codes[row['Cell_Code'][:3]] * 50000, axis=1)
        lte_zone_df['LAC'] += lte_zone_df.apply(lambda row: offset_codes[row['Cell_Code'][3]] * 10000, axis=1)
        lte_zone_df['LAC'] += lte_zone_df.apply(lambda row: int(row['Cell_Code'][4:8]), axis=1)
        lte_zone_df['LAC'] *= 1000
        # We recycle some variables
        front = lte_zone_df.loc[:, :'Longitude']
        column_mask = lte_zone_df.columns.str.contains('CID')
        updated_cid = lte_zone_df.loc[:, lte_zone_df.columns[column_mask]].add(lte_zone_df.LAC, axis=0)
        lte_zone_df = pd.concat([front, updated_cid], axis=1)
        reset_mask = (lte_zone_df.loc[:, column_mask].T >= lte_zone_df.LAC).T
        zone_cid = lte_zone_df.loc[:, column_mask][reset_mask].apply(pd.to_numeric).fillna(-1).astype(int)
        lte_zone_df = pd.concat([front, zone_cid], axis=1)

        zone_df = zone_df.append(lte_zone_df)

    return [name_string, zone_df]


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("file", type=str, help="Data to work with")
    parser.add_argument("-r", "--region", type=str,
                        help="Comma separared list of regions to extract")
    parser.add_argument("-m", "--municipality", type=str,
                        help="List of municipalities to extract (eg: 29,31,50)")
    args = parser.parse_args()

    # Change the input based on whether it's a local file or an s3 file
    file_path = args.file.split('/')
    if file_path[0] == 's3:':
        s3_file = True
        cell_df = pd.read_csv(fs.open(args.file), compression='gzip',
                              low_memory=False)
    else:
        s3_file = False
        cell_df = pd.read_csv(args.file, compression='gzip', low_memory=False)

    name_string, cell_df = extract_cell_df(cell_df, args.municipality, args.region)

    # Make a file for future use
    if s3_file:
        input_name = file_path[-1].split('_')
    else:
        input_name = args.file.split('_')
    if len(name_string) > 0:
        # Clean up trailing _
        if len(name_string.split('_')[-1]) == 0:
            name_string = name_string.split('_')
            name_string.pop()
            name_string = '_'.join(name_string)
        input_name[1] = name_string
    else:
        input_name.pop(1)
    output_name = '_'.join(input_name)
    cell_df.to_csv(output_name, compression='gzip', index=False)

    sys.exit()
