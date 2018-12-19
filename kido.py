import os
import random
import string
import tempfile
import datetime
import pandas as pd
# import geopandas as gpd
from shapely.geometry import Point

import s3fs
import boto3

fs = s3fs.S3FileSystem(anon=False)
s3 = boto3.resource('s3')

# Static data
DIR_AGES = 's3://orange-sthar/DATA/Edades2011/'
DIR_URBANNUC = 's3://orange-sthar/DATA/BTN100_Urban/'
DIR_CARTOGRAPHY = 's3://orange-sthar/DATA/cartografia_censo2011_nacional/'
DIR_PROV = 's3://orange-sthar/DATA/PROV/'
DIR_ROADS = 's3://orange-sthar/DATA/ROADS/'
MCC_FILE = '/data01/DATA/MCC_Continent.csv.gz'
DIR_HOLIDAYS = '/data01/DATA/holidays'

# Export lists
# Used for standardizing output creation
COLUMNS_TO_SAVE = ['TimeEpoch', 'Start Cell', 'Latitude', 'Longitude']
# The idea being that any output created should be done like:
# kido.columns_to_save += ['Country', 'Continent']
# dataframe_to_csv[kido.columns_to_save].to_csv(output_name, ...)

#
#
# Our functions
#
#

# These aren't necessarily used in every project
# but are general enough that are not specific to a company

def readAllinDir(path):
    local_path = path.split('/')
    dataframe = pd.DataFrame()
    if local_path[0] == 's3:':
        dir_list = fs.ls(path)
        for file in dir_list:
            local_df = pd.read_csv(fs.open(file), low_memory=False)
            dataframe = dataframe.append(local_df)
    else:
        dir_list = os.listdir(path)
        for file in dir_list:
            local_df = pd.read_csv(file, low_memory=False)
            dataframe = dataframe.append(local_df)
    return dir_list


def readAllCSVInPath(input_path, columns_to_keep=None):
    '''Returns a DataFrame with all CSV in one path.

    The path should be a string.
    The path can be both local or in s3.
    The path can be a directory or a file.'''
    path = input_path.split('/')
    # In case we have a final /
    if len(path[-1]) == 0:
            path.pop()
    if path[0] == 's3:':
        df = pd.DataFrame()
        bucket_name = path[2]
        bucket_path = path[3:]
        final_path = os.path.join(bucket_name, *bucket_path)
        file_list = fs.ls(final_path)
        for file in file_list:
            local_df = pd.read_csv(fs.open(file), compression='gzip', low_memory=False)
            if columns_to_keep is not None:
                local_df = local_df[columns_to_keep]
            df = df.append(local_df)
    else:
        # If a file was given
        if 'gz' in path[-1]:
            path = os.path.join(*path)
            df = pd.read_csv(path, compression='gzip', low_memory=False)
            if columns_to_keep is not None:
                df = df[columns_to_keep]
        else:
            os.chdir(path)
            for file in os.listdir():
                local_df = pd.read_csv(file, compression='gzip', low_memory=False)
                if columns_to_keep is not None:
                    local_df = local_df[columns_to_keep]
                df = df.append(local_df)
    return df

def make_safe_dir(path):
    '''Safely create a local directory.

       If the directory exists and is a file randomize name based on
       initial path.'''
    # In case we're somehow fed a number
    path = str(path)
    if not os.path.exists(path):
        os.makedirs(path, mode=0o700)
    else:
        # In case the path exists but it's a file we create a random path
        if not os.path.isdir(path):
            path += '_' + ''.join(random.choices(string.ascii_lowercase
                                  + string.digits, k=8))
            os.mkdir(path, mode=0o700)
    return path


def move_to_s3(input_path, destination_bucket, destination_path, keep=False):
    '''Function to move files to s3.

       If keep=True the file is also kept locally.'''
    s3.meta.client.upload_file(input_path, destination_bucket,
                               destination_path)
    if not keep:
        os.remove(input_path)
    return None


def dump_to_clean_cdr(dest_bucket, dataframe, year, month, day,
                      process_name, file_count, to_s3=False, final=False,
                      yesterday=False, region=False, output_dir='subset'):
    '''Dumps a cleaned up CDR file to a CSV.'''
    if not dataframe.empty:
        # These files get quite big, so we need to spread them over
        # all the small disks
        if int(process_name) % 2 == 0:
            prefix = '/tmp'
        else:
            prefix = os.path.join(os.getenv('HOME'), '.tmp_data/')
        # Now we make two paths. One for the local storage (split in multiple
        # small partitions) and the path for s3
        # If we're storing the files in s3 at the end, we can create a true
        # tmpdir. Otherwise a systematic name is necessary
        if to_s3:
            tmp_path = tempfile.mkdtemp(dir=prefix)
        else:
            tmp_path = os.path.join('CDRs', year, month, day)
            make_safe_dir(tmp_path)
        base_path = 'CDRs/clean_cdrs'
        # If we have just a region, we should dump to a subdirectory
        if region:
            output_path = os.path.join(base_path, str(year), str(month), str(day), output_dir)
        else:
            output_path = os.path.join(base_path, str(year), str(month), str(day))
        # To make clear that everything went fine and we have the last files.
        if final:
            final_str = '_final'
        else:
            final_str = ''
        # To avoid overwriting the standard files.
        if yesterday:
            output_name = ('yesterday_cdr_%s_%s.csv.gz' % (str(process_name),
                           str(file_count)))
        else:
            output_name = ('cdr_%s_%s%s.csv.gz' % (str(process_name),
                           str(file_count), final_str))
        # We first dump to a csv so we can compress it.
        local_path = os.path.join(tmp_path, output_name)
        dataframe.to_csv(local_path, compression='gzip', index=False)
        # "Directories" within the bucket get created automatically
        if to_s3:
            s3_path = os.path.join(output_path, output_name)
            move_to_s3(local_path, dest_bucket, s3_path)
            os.removedirs(tmp_path)
    return None


def add_to_name(input_name, string_to_add):
    '''Returns a name with a string appended.

       This eliminates everything beyond the first dot.'''
    output_path = input_name.split('/')[:-1]
    if not string_to_add.split('.')[-2:] == ['csv', 'gz']:
        output_name = input_name.split('/')[-1].split('.')[0] + string_to_add \
                        + '.csv.gz'
    else:
        output_name = input_name.split('/')[-1].split('.')[0] + string_to_add
    return os.path.join(*output_path, output_name)


def isPointInRegion(latitude, longitude, area_dataframe):
    """Returns Boolean for whether a point is within a polygon."""
    # We make the point with longitude/latitude because the source data comes
    # in this format
    position = Point(longitude, latitude)
    return any(area_dataframe.intersects(position))


def PointInWhichRegion(latitude, longitude, area_dataframe):
    """Returns the region to which point belongs."""
    # We make the point with longitude/latitude because the source data comes
    # in this format
    position = Point(longitude, latitude)
    local_result = 0
    zone_list = list(area_dataframe[area_dataframe.intersects(position)]['id'])
    if len(zone_list):
        local_result = zone_list[0]
    return local_result


def expand_arguments(municipalities=None, regions=None):
    """Returns a set of arguments for names and extraction data.

       name_string is to be used for naming output files.
       zone_data contains the zones to use.
       column_data contains either 'Region' or 'Municipality'
       full is used in case no values were given."""

    if (municipalities == None) and (regions == None):
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
    """Returns dataframe with subset of 'Region' or 'Municipality.'"""
    local_dataframe = cell_dataframe[cell_dataframe[column] == area]
    return local_dataframe


def make_zone_df(cell_dataframe, zone_data, column_data, full=False):
    """Returns a cell dataframe for a small zone."""
    zone_df = pd.DataFrame()
    if full:
        zone_df = cell_dataframe
    else:
        for i in range(len(zone_data)):
            zone_list = zone_data[i]
            for zone in zone_list:
                local_df = extract_zone(cell_dataframe, column_data[i],
                                        int(zone))
                zone_df = zone_df.append(local_df)
    return zone_df


def makeCIDset(cell_dataframe):
    """Returns a set with all the valid CID in the dataframe."""
    cid_set = set()
    cid_mask = cell_dataframe.columns.str.contains('CID')
    for column in cell_dataframe.columns[cid_mask]:
        cid_set = cid_set | set(cell_dataframe[column])
        if {-1}.issubset(cid_set):
            cid_set.remove(-1)
    return cid_set


def isDayHoliday(holiday_dataframe, year, month, day, region=0, country='spain'):
    """Returns a boolean on weather a day is an holiday.

       If region is given, check if it's a holiday for that specific region.
       If country is given, check that country."""
    holiday = not holiday_dataframe[(holiday_dataframe.month == month)
                            & (holiday_dataframe.day == day)
                            & (holiday_dataframe.region == region)].empty
    return holiday


# PROBABLY deprecated!
def fromTimestampToPeriod(timestamp, isHoliday):
    """Returns a string identifying a time period."""
    # The last two are: day of year, daylight savings time
    year, month, day, hour, min, sec, weekday, doy, dst = datetime.datetime.timetuple(datetime.datetime.utcfromtimestamp(timestamp))

    # Separate by Time into sections:
    # - work time Mon-Thu: 8:00-19:59, Fri: 8:00-15:59)
    # - weekend Fri: 16:00-19:59, Sat: 10:00-19:59, Sun: 11:00-19:59
    # - holiday == Sun
    # - night time (everything else)

    # If it's a holiday we change weekday = 6
    if isHoliday:
        weekday = 6
    # We set period to night by default to save on else clauses
    period = 'night'
    # Mon = 0, Tue = 1, ...
    if weekday < 4:
        if (hour >= 8) and (hour < 20):
            period = 'work'
    # Friday
    if weekday == 4:
        if (hour >= 8) and (hour < 16):
            period = 'work'
        if (hour >= 16) and (hour < 20):
            period = 'weekend'
    # Saturday
    if weekday == 5:
        if (hour >= 10) and (hour < 20):
            period = 'weekend'
    # Sunday
    if weekday == 6:
        if (hour >= 11) and (hour < 20):
            period = 'weekend'
    return period


def getTimePeriod(weekday, hour):
    """Returns a string identifying a time period."""
    period = 'night'
    # For a project with Cintra (may not be of much use elsewhere)
    # Separate by Time into sections:
    # - work time Mon-Thu: 8:00-19:59, Fri: 8:00-15:59)
    # - weekend Fri: 16:00-19:59, Sat: 10:00-19:59, Sun: 11:00-19:59
    # - holiday == Sun
    # - night time (everything else)
    # Mon = 0, Tue = 1, ...
    if weekday < 4:
        if (hour >= 8) and (hour < 20):
            period = 'work'
    # Friday
    if weekday == 4:
        if (hour >= 8) and (hour < 16):
            period = 'work'
        if (hour >= 16) and (hour < 20):
            period = 'weekend'
    # Saturday
    if weekday == 5:
        if (hour >= 10) and (hour < 20):
            period = 'weekend'
    # Sunday
    if weekday == 6:
        if (hour >= 11) and (hour < 20):
            period = 'weekend'
    return period


def makeDateHourColumns(cdr_dataframe):
    '''Returns a DataFrame with extra columns for Year, Month, Day and Hour.'''
    cdr_dataframe['Year'] = cdr_dataframe['Date'] // 10000
    cdr_dataframe['Month'] = (cdr_dataframe['Date'] - cdr_dataframe['Year'] * 10000) // 100
    cdr_dataframe['Day'] = (cdr_dataframe['Date'] - cdr_dataframe['Year'] * 10000
                       - cdr_dataframe['Month'] * 100)
    cdr_dataframe['Hour'] = cdr_dataframe['Time'] // 10000
    # Probably we don't need minute/second
    return cdr_dataframe


def GetAllAtPosition(cell_dataframe, latitude, longitude):
    """Returns the dataframe of all cells at the same latitude/longitude."""
    return cell_dataframe[(cell_dataframe['Latitude'] == latitude) & (cell_dataframe['Longitude'] == longitude)]


def makeCIDLatLon(cell_dataframe):
    """Returns an expanded dataframe with only three components CID, Latitude, Longitude"""
    full_df = pd.DataFrame()
    for cid in cell_dataframe.loc[:, 'CID_1':].columns:
        local_df = cell_dataframe.loc[:, ['Latitude', 'Longitude', cid]]
        local_df.columns = ['Latitude', 'Longitude', 'Cell ID']
        local_df = local_df[local_df['Cell ID'] > 0]
        full_df = full_df.append(local_df)
    return full_df


def read_csv(input_file):
    """Returns a DataFrame that is read from one of our (CDR) files.

       It controls for whether the file is in a S3 bucket or not."""
    input_path = input_file.split('/')
    if input_path[0] == 's3:':
        bucket_name = input_path[2]
        bucket_path = input_path[3:]
        file = os.path.join(bucket_name, *bucket_path)
        local_df = pd.read_csv(fs.open(file), compression='gzip',
                             low_memory=False)
    else:
        local_df = pd.read_csv(input_file, compression='gzip', low_memory=False)
    return local_df

def write_csv(dataframe, output_file, separator=',', add_index=False):
    """Creates a CSV file with gzip compression of the dataframe."""
    if not output_file.endswith('.gz'):
        output_file += '.gz'
    dataframe.to_csv(output_file, compression='gzip', index=add_index,
                     sep=separator)
