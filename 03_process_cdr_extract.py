#!/usr/bin/env python3.6

import os
import sys
import argparse
import datetime

import pandas as pd
# import numpy as np
import s3fs
import boto3
import random

import multiprocessing as mp
import time

import kido
import orange

fs = s3fs.S3FileSystem(anon=False)
# It's easier to copy files *to* s3 with boto3
s3 = boto3.resource('s3')

DATA_BUCKET = 'orange-sthar'


def dump_to_file(dest_bucket, dataframe, path, name, count, to_s3=False,
                 final=False, yesterday=False):
    # We need to make sure output_path exists
    if not dataframe.empty:
        if args.output is not None:
            path = os.path.join(path, args.output)
        output_path = kido.make_safe_dir(path)
        output_count = count
        if final:
            final_str = '_final'
        else:
            final_str = ''
        if yesterday:
            output_name = ('yesterday_%s_%s%s.csv.gz' % (name, output_count,
                           final_str))
        else:
            output_name = ('output_%s_%s%s.csv.gz' % (name, output_count,
                       final_str))
        # We first dump to a csv so we can compress it.
        final_path = os.path.join(output_path, output_name)
        dataframe.to_csv(final_path, compression='gzip', index=False)
        # "Directories" within the bucket get created automatically
        #if to_s3:
        #    kido.move_to_s3(final_path, dest_bucket, final_path)
    return None


def process_data(dest_bucket, date_string_list, section_size, foreigners_only=False,
                 region_cell_dataframe=None, create_epoch=False):
    process_int = int(mp.current_process().name)
    cdr_df = pd.DataFrame()
    yesterday_cdr_df = pd.DataFrame()
    # YYYYMMDD
    year = int(date_string_list[0])
    month = int(date_string_list[1])
    day = int(date_string_list[2])
    previous_day = datetime.date.fromordinal(datetime.date(year, month, day).toordinal() - 1).timetuple()[0:3]
    yesterday = previous_day[0] * 10000 + previous_day[1] * 100 + previous_day[2]
    # Make the output paths
    output_path = os.path.join('CDRs', *date_string_list)
    yesterday_output_path = os.path.join('CDRs', '{:4}'.format(previous_day[0]),
                                         '{:02}'.format(previous_day[1]),
                                         '{:02}'.format(previous_day[2]))
    file_count = 0
    output_count = 0
    # We randomize section_size to avoid that all processes are writing at the
    # same time
    variability = .15
    max_cut = int(section_size * variability)
    min_cut = int(max_cut / 10)
    random_cut = random.randint(min_cut, max_cut)

    # Some sections should be bigger, some smaller to distribute the writes
    if process_int % 2:
        random_cut *= -1
    section_size += random_cut

    cells_in_zone = set()
    for column in region_cell_df.loc[:, 'CID_1':]:
        cells_in_zone = cells_in_zone | set(region_cell_df[column])
    if {-1}.issubset(cells_in_zone):
        cells_in_zone.remove(-1)

    while not q.empty():
        # We use this to limit the amount of memory used per process
        # We should probably limit based on the size of the DataFrame per
        # process with something like:
        # cdr_df.shape[0] > 20000000 (20M ~= 1 GB file)
        if file_count < section_size:
            data = q.get()
            local_df = pd.DataFrame(orange.read_cdr_file(data, foreigners_only,
                                    cells_in_zone, create_epoch))
            # We saw events from two (!) days before in some files so we need
            # to throw away anything older than yesterday
            old_mask = local_df['Date'] < yesterday
            local_df = local_df[~old_mask]
            yesterday_mask = local_df['Date'] == yesterday
            yesterday_cdr_df = yesterday_cdr_df.append(local_df[yesterday_mask])
            cdr_df = cdr_df.append(local_df[~yesterday_mask])
            file_count += 1
        else:
            dump_to_file(dest_bucket, cdr_df, output_path, process_int,
                         output_count, True)
            cdr_df = pd.DataFrame()
            file_count = 0
            output_count += 1
    dump_to_file(dest_bucket, cdr_df, output_path, process_int, output_count,
                 True, True)
    dump_to_file(dest_bucket, yesterday_cdr_df, yesterday_output_path,
                 process_int, 0, True, False, True)
    return None


if __name__ == '__main__':

    # Parse the input
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--np", default=4, type=int,
                        help="Number of parallel processes to use")
    parser.add_argument("-s", "--section_size", default=500, type=int,
                        help="Number of files to read before dumping to disk")
    parser.add_argument("-r", "--region_file", type=str,
                        help="Cell DataFrame for the region to work with for a first filtering")
    parser.add_argument("-f", "--foreigners", action='store_true',
                        help="Keep only data for foreigners (other filters may apply as well)")
    parser.add_argument("-e", "--epoch", action='store_true',
                        help="Modify Date/Time to Epoch (deleting Date in the process).")
    parser.add_argument("-o", "--output", type=str,
                        help="Name of the directory files will be saved to.")
    parser.add_argument("path", type=str,
                        help="Data to work with (should be s3 bucket)")
    args = parser.parse_args()

    # Paths to work with
    SOURCE_BUCKET, PREFIX, YEAR, MONTH, DAY, CDR_LIST = orange.prepare_cdr_path(args.path)
    input_path = PREFIX
    # output_path = os.path.join('CDRs', YEAR, MONTH, DAY)
    today = [YEAR, MONTH, DAY] # These are strings

    # Read the cell_df for the region of interest
    if args.region_file is not None:
        region_file_path = args.region_file.split('/')
        if region_file_path[0] == 's3:':
            region_cell_df = pd.read_csv(fs.open(args.region_file),
                                         compression='gzip', low_memory=False)
        else:
            region_cell_df = pd.read_csv(args.region_file, compression='gzip',
                                         low_memory=False)
    else:
        region_cell_df = pd.DataFrame()

    list_of_files = []
    q = mp.Queue()
    # Can we do this without recreating the processes?
    print("Getting files from s3://%s" % os.path.join(SOURCE_BUCKET,
                                                      input_path))
    time_start = time.time()
    for cdr in CDR_LIST:
        final_input_path = os.path.join(SOURCE_BUCKET, input_path, cdr)
        list_of_files += fs.ls(final_input_path)

    print("List of files generated in %.1fs. It contains %d files." %
          (time.time() - time_start, len(list_of_files)))
    jobs = []
    # We need a queue to make sure processes aren't doing the same stuff
    for file in list_of_files:
        q.put(file)
    time_real_start = time.time()
    for i in range(args.np):
        p = mp.Process(name=str(i), target=process_data,
                       args=(DATA_BUCKET, today, args.section_size,
                             args.foreigners, region_cell_df, args.epoch))
        jobs.append(p)
        p.start()

    for p in jobs:
        p.join()

    print('Total time: ' + str(time.time() - time_start))
    print('Exiting Main Process')
    sys.exit()
