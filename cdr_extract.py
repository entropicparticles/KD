#!/usr/bin/env python3.6

import argparse
import os
import sys
import time

import multiprocessing as mp

import numpy as np
import pandas as pd
import random

import s3fs
import boto3

import orange

fs = s3fs.S3FileSystem(anon=False)
# It's easier to copy files *to* s3 with boto3
s3 = boto3.resource('s3')

DATA_BUCKET = 'orange-sthar'

def dump_to_s3(dest_bucket, dataframe, name, path, count, final=False):
    # We need to make sure output_path exists
    output_path = orange.make_safe_dir(path)
    if final:
        final_str='_final'
    else:
        final_str=''
    output_name = ('output_%s_%s%s.csv.gz' % (name, count, final_str))
    # We first dump to a csv so we can compress it.
    dataframe.to_csv(os.path.join(output_path, output_name), compression='gzip', index=False)
    # "Directories" within the bucket get created automatically
    s3.meta.client.upload_file(os.path.join(output_path, output_name), dest_bucket, os.path.join(output_path, output_name))
    os.remove(os.path.join(output_path, output_name))
    return None

def write_data(dest_bucket, output_path, section_size):
    '''Gets information from queue and writes it to a file'''
    process_int = mp.current_process().name
    file_count = 0
    output_count = 0
    cdr_df = pd.DataFrame()
    while True:
        data = write_q.get()
        if type(data) == pd.core.frame.DataFrame:
            if file_count < section_size:
                cdr_df = cdr_df.append(data)
                file_count += 1
                if not (file_count % 50):
                   print("The writer %s is accumulating: %d" % (process_int, file_count))
                   print("Size of queue is: %d" % write_q.qsize())
            else:
                print("The writer is writing file: %d" % output_count)
                dump_to_s3(dest_bucket, cdr_df, process_int, output_path, output_count)
                cdr_df = pd.DataFrame()
                file_count = 0
                output_count += 1
                print("Size of queue is: %d" % write_q.qsize())
        else:
            break

    if not cdr_df.empty:
        dump_to_s3(dest_bucket, cdr_df, output_path, output_count, True) 
    return None

def process_data():
    '''Reads the data from the original CSV files and processes it into a useable format'''
    process_int = int(mp.current_process().name)
    cdr_df = pd.DataFrame()
    file_count = 0
    output_count = 0
    
    while not file_q.empty():
        data = file_q.get()
        write_q.put(orange.read_cdr_file(data))

    return None

if __name__ == '__main__':

    # Parse the input
    parser = argparse.ArgumentParser()
    parser.add_argument("np", default=4, type=int, help="Number of parallel processes to use")
    parser.add_argument("path", type=str, help="Data to work with (should be s3 bucket)")
    parser.add_argument("section_size", default=2000, type=int, help="Number of files to read before dumping to disk")
    args = parser.parse_args()

    # Paths to work with
    SOURCE_BUCKET, PREFIX, YEAR, MONTH, DAY, CDR_TYPE, COMPLETE_PATH = orange.prepare_cdr_path(args.path)
    # Sometimes Orange puts the directories on the wrong places
    # so we need to sanitize the input
    if COMPLETE_PATH:
        input_path = os.path.join(PREFIX, YEAR, MONTH, DAY)
    else:
        input_path = PREFIX
        
    output_path = os.path.join('CDRs', YEAR, MONTH, DAY)

    # A smaller section_size means each individual file is smaller, so we don't risk filling /home
    section_size = args.section_size

    list_of_files = []

    time_main = time.time()

    # A queue for all the inputs and a queue for the writer processes
    file_q = mp.Queue()
    # The limiting step is the writing, so we don't need write_q to be too big
    write_q = mp.Queue(maxsize=500)

    time_start = time.time()
    for cdr in CDR_TYPE:
        final_input_path = os.path.join(SOURCE_BUCKET, input_path, cdr)
        list_of_files += fs.ls(final_input_path)

    print("List of files generated in %.1fs. Now the fun starts." % (time.time() - time_start))
    writer_jobs = []
    processor_jobs = []
    # We need a queue to make sure processes aren't doing the same stuff
    for file in list_of_files:
        file_q.put(file)

    # Create the write processes (we need around 80% of writer processes)
    write_processes = args.np - 1
    for i in range(0, write_processes):
        writer_p = mp.Process(name=str(i), target=write_data, args=(DATA_BUCKET, output_path, section_size))
        writer_jobs.append(writer_p)
        writer_p.start()


    # We already used one process for the writer
    for i in range(write_processes, args.np):
        p = mp.Process(name=str(i), target=process_data)
        processor_jobs.append(p)
        p.start()
    
    for p in processor_jobs:
        p.join()

    write_q.put('kill_writer')

    for writer in writer_jobs:
        writer_p.join()
    
    print('Total time: %.1fs' % (time.time() - time_main))
    print("Exiting Main Process")
    sys.exit()
