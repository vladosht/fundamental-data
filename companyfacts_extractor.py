#!/usr/bin/env python3
# %%
from contextlib import redirect_stdout
import os, sys, warnings, io, gc, argparse
import cProfile, pstats
import zipfile, json, itertools
import datetime, regex as re
import pandas as pd
import numpy as np
from functools import cache
from numba import njit, prange
import concurrent.futures
warnings.filterwarnings("error")  #Enforce a kind of a 'strict' mode.

# %%
def parse_arguments():

    description = """
    Reduce a SEC bulk financial data companyfacts.zip file to a time-series CSV.
    This zip file is freely downloadable from the following web page of
    the United States Securities and Exchange Commission:
    https://www.sec.gov/search-filings/edgar-application-programming-interfaces

    The input .zip binary must be provided on the standard input.
    The output CSV is written as utf-8 plain text to the standard output.
    All status messages are logged to stderr only.

    If a company_tickers_exchange.json file is found in the working directory,
    it will be used to populate the output dataset with the corresponding stock
    exchange names and ticker symbols of the SEC cik IDs present in the output CSV.
    This file is also freely downloadable from the following SEC link:
    https://www.sec.gov/files/company_tickers_exchange.json

    This program utilizes multiprocessing via the legacy backend of python's
    joblib module. Therefore, unless explicitly switched off, such kind of parallelism
    may prevent the program from running correctly in some container environments.
    """

    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--max-jobs", "-j",
        type=int,
        default=-1,
        help="Number of jobs to run in parallel. Setting this to 1 disables multiprocessing and enables profiling. Default is -1 (use all CPU cores/threads)."
    )

    parser.add_argument(
        "--partial-dataset", "-p",
        action="store_true",
        help="Process only a small subset of the data for debugging or profiling purposes."
    )

    parser.add_argument(
        "--dump-intermediate-stages", "-d",
        action="store_true",
        help="Save the intermediate companyfacts CSV dataset and the individual snapshot pivots to the current working directory."
    )

    args = parser.parse_args()
    if args.max_jobs < 1:
        args.max_jobs = -1
    args.vCPUs = max(1,os.cpu_count()) if args.max_jobs < 1 else args.max_jobs

    return args

# %%
# There is a very large number of XBRL tags present in the zip file from the SEC.
# We are only interested in a few of them, that will be used to construct the end results.
known_facts = [
    'dei.EntityCommonStockSharesOutstanding.shares',
    'dei.EntityNumberOfEmployees.person',
    'dei.EntityPublicFloat.usd',
    'us-gaap.AssetsCurrent.usd',
    'us-gaap.AssetsNoncurrent.usd',
    'us-gaap.Assets.usd',
    'us-gaap.CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations.usd',
    'us-gaap.CostOfGoodsAndServicesSold.usd',
    'us-gaap.CostOfGoodsSold.usd',
    'us-gaap.CostOfRevenue.usd',
    'us-gaap.CostOfServices.usd',
    'us-gaap.GrossProfit.usd',
    'us-gaap.InterestAndDividendIncomeOperating.usd',
    'us-gaap.InterestAndFeeIncomeLoansAndLeases.usd',
    'us-gaap.LiabilitiesAndStockholdersEquity.usd',
    'us-gaap.NetCashProvidedByUsedInFinancingActivitiesContinuingOperations.usd',
    'us-gaap.NetCashProvidedByUsedInFinancingActivities.usd',
    'us-gaap.NetCashProvidedByUsedInOperatingActivitiesContinuingOperations.usd',
    'us-gaap.NetCashProvidedByUsedInOperatingActivities.usd',
    'us-gaap.NetIncomeLossAttributableToNoncontrollingInterest.usd',
    'us-gaap.NetIncomeLossAvailableToCommonStockholdersBasic.usd',
    'us-gaap.NetIncomeLoss.usd',
    'us-gaap.NoninterestIncome.usd',
    'us-gaap.OtherIncome.usd',
    'us-gaap.OtherSalesRevenueNet.usd',
    'us-gaap.ProfitLoss.usd',
    'us-gaap.RevenueFromContractWithCustomerExcludingAssessedTax.usd',
    'us-gaap.RevenueFromContractWithCustomerIncludingAssessedTax.usd',
    'us-gaap.Revenues.usd',
    'us-gaap.SalesRevenueGoodsNet.usd',
    'us-gaap.SalesRevenueNet.usd',
    'us-gaap.SalesRevenueServicesNet.usd',
    'us-gaap.StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest.usd',
    'us-gaap.StockholdersEquity.usd',
    'us-gaap.WeightedAverageNumberOfDilutedSharesOutstanding.shares',
    'us-gaap.WeightedAverageNumberOfShareOutstandingBasicAndDiluted.shares',
    'us-gaap.WeightedAverageNumberOfSharesOutstandingBasic.shares'
]

# Enforce consistency of the output CSV structure to not break applications that use the resulting dataset.
# If new columns are introduced, they will be appended to the end of this list.
export_schema = [
    'snapshot',
    'cik',
    'date',
    'Assets',
    'Revenue',
    'COGS',
    'GrossProfit',
    'Equity',
    'NetCashOperating',
    'NetCashFinancing',
    'Earnings',
    'Shares',
    'Liabilities',
    'Revenue_ttm',
    'GrossProfit_ttm',
    'NetCashOperating_ttm',
    'NetCashFinancing_ttm',
    'Earnings_ttm',
    'ticker',
    'exchange',
    'PublicFloat',
    'Employees'
]

# Configuration of the optimized json pre-processor
# Needed as string, because the functools cache accepts only
# immutable types as parameters
unwanted_keys = ['accn','fp','fy','form','frame']
json_preprocessor_config = json.dumps((known_facts, unwanted_keys))

# %%
def dump_memory_usage(note=None,top_n=5):
    sizes = sorted([round(sys.getsizeof(obj)/2**30,3) for obj in gc.get_objects()],reverse=True)[:top_n]
    print(f"{note}:" if note is not None else '',
          f"{round(sum(sizes),1)}GiB used by the top {top_n} memory objects:", sizes, file=sys.stderr)

# %%
@cache  #Executed only once, because config_json never changes during a program run
def get_re_patterns(config_json):
    known_facts, unwanted_keys = json.loads(config_json)

    # Convert the lists of strings to binary strings of names, joined by the OR regex operator '|'
    known_facts =   b'|'.join([re.escape(a_fact.split('.')[1].encode()) for a_fact in known_facts])
    unwanted_keys = b'|'.join([re.escape(a_key.encode()) for a_key in unwanted_keys])

    # This is how a beginning of a wanted json slice looks like.
    # We are trying to match these for all known facts and remove the others.
    """
    "Assets":{"label":"Assets",
              "description":"Sum of the carrying amounts as of the balance sheet date of all assets that are recognized...",
              "units":{"USD":[{"end":"2024-08-31","val":2783300000,"accn":"0001410578-24-001617","fy":2025,"fp":"Q1","form":"10-Q","filed":"2024-09-24","frame":"CY2024Q3I"},
                              {"end":"2024-11-30","val":2849300000,"accn":"0001410578-25-000003","fy":2025,"fp":"Q2","form":"10-Q","filed":"2025-01-08","frame":"CY2024Q4I"},
                              {"end":"2025-02-28","val":2859100000,"accn":"0001410578-25-000519","fy":2025,"fp":"Q3","form":"10-Q","filed":"2025-03-28","frame":"CY2025Q1I"},
                              ...
    This corresponds to the 'us-gaap.Assets.usd' known fact.
    """

    # Pass 1: Keep wanted facts, remove others
    # (*SKIP)(*FAIL) prevents the regex engine from matching inside target blocks
    fact_body = rb'":\{"label":[^]]*\]\}\}'
    keep_part = b',"(?:' + known_facts + b')' + fact_body
    drop_part = rb',"\w+' + fact_body
    facts_regex = re.compile(keep_part + rb'(*SKIP)(*FAIL)|' + drop_part)

    # Pass 2: Mass-remove unwanted keys and their values (can be either strings or integers)
    values_of_keys = rb')":(?:"[^"]*"|[^,}]+)'
    keys_regex = re.compile(rb',"(' + unwanted_keys + values_of_keys)

    return facts_regex, keys_regex

# Initialize the cache with the only value it will ever get. This is a precaution against
# concurrent access problems
get_re_patterns(json_preprocessor_config)

# %%
def preprocess_json(jfile, config_json):
    facts_regex, keys_regex = get_re_patterns(config_json) # Get the cached regex patterns
    jfile = facts_regex.sub(b'', jfile) #Remove unwanted facts
    jfile = keys_regex.sub(b'', jfile)  #Remove unwanted data fields
    return jfile

# %%
def reduce_a_json_dict(jdict):
    """
    We take the parsed json data and do data reduction/validation here,
    because we do not need most of the content
    """
    j = list() #Always return a list, for simplified downstream logic

    # Basic sanity checks for the json contents. These checks fail with a KeyError
    try:
        cik = str(jdict['cik']).zfill(10)  #A cik must be present
        if not len(jdict['facts']['us-gaap'].keys()) >= 2:  #At least this many distinct facts must be present
            raise KeyError('No actual data found in json file.')
    except KeyError: # A valid json file with no usable data is not an error. Silently skipping this file...
        return j

    for fact_path in known_facts:
        taxonomy, fact, wanted_units = fact_path.split('.')
        wanted_units = [ wanted_units.lower() ]
        if 'person' in wanted_units:
            wanted_units += ['employ','colleague','staff','count','item' ]
        # Test if the wanted unit(s) like USD, shares and people, are present in the json data for the current taxonomy fact.
        try:
            wanted_units = [ a_unit for a_unit in jdict['facts'][taxonomy][fact]['units'].keys() if a_unit.lower() in wanted_units ]
        except:
            continue
        # Use the first unit found which contans data
        for a_unit in wanted_units:
            fact_data = jdict['facts'][taxonomy][fact]['units'][a_unit]
            if fact_data:
                j += list(map(lambda x:x | {'cik': cik, 'fact': fact_path }, fact_data))
                break
    return j

# %%
def dicts_to_pandas(parsed_data:list):
    #Always return a DataFrame for easier post-processing
    df = pd.DataFrame(parsed_data)

    if not df.empty:
        # For some facts the start date is not given, but the downstream logic depends on it.
        if 'start' not in df.columns:
            df['start'] = df['end']
        else:
            df['start'] = df['start'].combine_first(df['end'])
        data_column_names = ['filed','cik','end','start','fact','val'] #There are other columns in the data we do not need
        df = df[data_column_names].dropna() #Drop the few facts that are present, but have no value

    if not df.empty:
        # Sanitize the datatypes
        df['val'] = pd.to_numeric(df['val'], errors='coerce')
        for a_date_column in ['start','end','filed']:
            df[a_date_column] = pd.to_datetime(df[a_date_column], yearfirst=True, errors='coerce')
        df['quarter_count'] = (pd.to_timedelta(df['end'] - df['start']).dt.days / 7 / 13).round(0).astype(int)

        # Filings for future periods may distort downstream processing, so we remove them. About 80% of these records are dei.* facts, anyway.
        # Reporting periods longer than four quarters also cause problems
        df = df[(df['start'] <= df['end']) & (df['end'] <= df['filed']) & ~(df['quarter_count']>4)]

    if not df.empty:
        # We treat all columns except 'val' as key. They should generally be unique, because a company should not report twice per day
        # divergent values for the same fact. We do aggregate here along the key however, just in case this is not true.
        data_column_names.remove('val')
        data_column_names.append('quarter_count')
        df = df.groupby(by=data_column_names)['val'].last()  #This also sorts the key
        # Many companies have submitted wrong or invalid values during the years. Most of these invalid values have been corrected eventually
        # with later filings. For each cik/period/fact combination, we determine the final available filing, so that the latest and most correct
        # value is used.
        final_values = df.droplevel('filed')
        final_values = final_values[~final_values.index.duplicated(keep='last')]
        df = pd.merge(df, final_values, left_index=True, right_index=True, suffixes=(None, '_final'))
        df['corrected'] = df['val'] != df['val_final']

    return df

def parse_json_batched(jlist):
    """
    Takes a list of uncompressed binary json files.
    Returns a pandas DataFrame
    """
    jlist = b'[' + b','.join(jlist) + b']'  #binary string
    jlist = preprocess_json(jlist, json_preprocessor_config) # Data reduction => less RAM and better performance.
    jlist = json.loads(jlist) #reduced binary string
    jlist = itertools.chain.from_iterable(map(reduce_a_json_dict, jlist)) #list of pruned dicts with the same schema
    jlist = dicts_to_pandas(jlist) #A single DataFrame
    min_cik, max_cik, count_cik = pd.Series(jlist.index.get_level_values('cik')).agg(['min','max','count'])
    if count_cik % 3 == 0: #Print only a third of the times to not clutter stderr
        print(f'CIKs from {min_cik} to {max_cik} completed', file=sys.stderr)
    return jlist

# %%
def make_companyfacts(args, batch_size = 100):
    # Here the list of files can be restricted for test and debugging purposes,
    # because the whole zip is large and takes time to process.
    debugging_slice = slice(None) if not args.partial_dataset else slice(0,2000)

    # Load the SEC zip file from standard input
    print("Reading standard input...")
    source_zip = sys.stdin.buffer.read()
    print(f"{len(source_zip)} bytes read.")
    source_zip = zipfile.ZipFile(io.BytesIO(source_zip), mode='r')
    files_to_parse = source_zip.namelist()[debugging_slice]

    dump_memory_usage(f'Parsing {len(files_to_parse)} json files in batches of {batch_size}')

    # These are 'lazy' executions, so that we do not decompress the whole zip into memory all at once.
    batches = itertools.batched(map(source_zip.read, files_to_parse), batch_size)

    job_results = concurrent_map_fn(parse_json_batched, batches)
    job_results = pd.concat(job_results)

    print(f'Financial data for {job_results.index.get_level_values("cik").nunique()} ciks was extracted.')
    source_zip.close()
    return job_results

# %%
def merge_facts(pivot):
    """
    The individual XBRL tags are combined into new columns, which carry the final business meaning.
    """
    # Some facts were introduced later during the years and for earlier snapshots
    # we must add them manually to avoid KeyErrors
    missing_facts = set(known_facts).difference(pivot.columns.to_list())
    for a_missing_fact in missing_facts:
        pivot[a_missing_fact] = np.nan

    def combine_columns(df):
        return df.sum(min_count=1, axis='columns')

    new_columns = list()

    new_columns.append(
        pivot['us-gaap.Assets.usd'].combine_first(
        pivot['us-gaap.LiabilitiesAndStockholdersEquity.usd']).combine_first(combine_columns(
        pivot[['us-gaap.AssetsCurrent.usd',
               'us-gaap.AssetsNoncurrent.usd',
        ]])
    ).rename('Assets'))

    new_columns.append(
        pivot['us-gaap.Revenues.usd'].combine_first(
        pivot['us-gaap.SalesRevenueNet.usd']).combine_first(combine_columns(
        pivot[['us-gaap.SalesRevenueGoodsNet.usd',
               'us-gaap.SalesRevenueServicesNet.usd',
               'us-gaap.OtherSalesRevenueNet.usd']])).combine_first(
        pivot['us-gaap.RevenueFromContractWithCustomerExcludingAssessedTax.usd']).combine_first(
        pivot['us-gaap.RevenueFromContractWithCustomerIncludingAssessedTax.usd']).combine_first(combine_columns(
        pivot[['us-gaap.InterestAndDividendIncomeOperating.usd',
               'us-gaap.InterestAndFeeIncomeLoansAndLeases.usd',
               'us-gaap.NoninterestIncome.usd',
               'us-gaap.OtherIncome.usd']])
    ).rename('Revenue'))

    new_columns.append(
        pivot['us-gaap.CostOfRevenue.usd'].combine_first(
        pivot['us-gaap.CostOfGoodsAndServicesSold.usd']).combine_first(combine_columns(
        pivot[['us-gaap.CostOfGoodsSold.usd',
               'us-gaap.CostOfServices.usd']])
    ).rename('COGS'))

    new_columns.append(
        pivot['us-gaap.GrossProfit.usd'].rename('GrossProfit'))

    new_columns.append(
        pivot['us-gaap.StockholdersEquity.usd'].combine_first(
        pivot['us-gaap.StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest.usd']
    ).rename('Equity'))

    new_columns.append(
        pivot['us-gaap.NetCashProvidedByUsedInOperatingActivities.usd'].combine_first(combine_columns(
        pivot[['us-gaap.NetCashProvidedByUsedInOperatingActivitiesContinuingOperations.usd',
               'us-gaap.CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations.usd'
        ]])
    ).rename('NetCashOperating'))

    new_columns.append(
        pivot['us-gaap.NetCashProvidedByUsedInFinancingActivities.usd'].combine_first(
        pivot['us-gaap.NetCashProvidedByUsedInFinancingActivitiesContinuingOperations.usd']
    ).rename('NetCashFinancing'))

    new_columns.append(
        pivot['us-gaap.NetIncomeLoss.usd'].combine_first(
        pivot['us-gaap.ProfitLoss.usd']).combine_first(
        pivot['us-gaap.NetIncomeLossAttributableToNoncontrollingInterest.usd']).combine_first(
        pivot['us-gaap.NetIncomeLossAvailableToCommonStockholdersBasic.usd']
    ).rename('Earnings'))

    new_columns.append(
        pivot['us-gaap.WeightedAverageNumberOfShareOutstandingBasicAndDiluted.shares'].combine_first(
        pivot['us-gaap.WeightedAverageNumberOfDilutedSharesOutstanding.shares']).combine_first(
        pivot['us-gaap.WeightedAverageNumberOfSharesOutstandingBasic.shares']
    ).rename('Shares'))

    new_columns.append(
        pivot['dei.EntityCommonStockSharesOutstanding.shares'].rename('EntityShares'))

    new_columns.append(
        pivot['dei.EntityPublicFloat.usd'].rename('PublicFloat'))

    new_columns.append(
        pivot['dei.EntityNumberOfEmployees.person'].rename('Employees'))

    new_columns = pd.concat(new_columns, axis='columns')

    return new_columns

# %%
def enrich_with_tickers(snapshots, tickers_info):
    if tickers_info is None:
        snapshots['ticker'] = ''
        snapshots['exchange'] = ''
        return snapshots
    return snapshots.merge(tickers_info[['ticker','exchange']],how='left',left_index=True,right_index=True)

# %%
@njit(nogil=True) # Numba is the speed king!
def scan_periods_to_dict(index_arr):
    idx, cik, start, end = (0,1,2,3) #Readable names for the columns of the lookup array
    row_count, column_count = (0,1)  #Readable names for the shape of the array

    output = dict()  #This is a numba dict, not a python dict! It behaves differently...

    cik_start_i = 0  #Input array is already sorted by cik, we take advantage of it here
    for i in range(1, index_arr.shape[row_count]):
        this_period = index_arr[i]

        # Make sure we operate only on rows with the same cik.
        if index_arr[cik_start_i][cik] != this_period[cik]:
            cik_start_i = i
            continue

        # Here we enforce the following logic:
        # same cik AND index_earlier_period < this_index
        earlier_periods = index_arr[cik_start_i:i]

        # start_earlier_period >= start AND end_earlier_period <= end
        earlier_periods = earlier_periods[(earlier_periods[:,start] >= this_period[start])&(earlier_periods[:,end] <= this_period[end])]

        if earlier_periods.shape[row_count] > 0:
            output[this_period[idx]] = earlier_periods[:,idx]

    return output

# %%
def process_single_pivot(a_date, a_pivot, args):
    # We need a new order of the keys, so that sorting along them will make possible the do_subtract logic below.
    a_pivot = a_pivot.reorder_levels(['cik','fact','start','end','quarter_count','filed']).sort_index().droplevel('filed', axis='index')
    a_pivot = a_pivot[~a_pivot.index.duplicated(keep='last')].unstack(level='fact').droplevel(0, axis='columns')
    # The index loses its sort order from above after the unstack operation.
    # This breaks the logic below, so here we explicitly sort the index again.
    a_pivot.sort_index(inplace=True,ascending=True)
    # The dei facts are almost always reported for dates unlike those for us-gaap facts. Thus, they require special treatment.
    # We save these columns for later.
    dei_facts = a_pivot.filter(regex=r'^dei\.Entity', axis='columns').copy()
    # Combine the individual fact columns into aggregate columns and treat the quarter count as a regular data column for later use.
    a_pivot = merge_facts(a_pivot).reset_index(level='quarter_count')

    # The data in the SEC file has been submitted as relevant for different period lengths, usually from 1 quarter to 1 year.
    # Here we convert all data to per-quarter data by subtracting earlier periods from the later, larger ones, which contain them.
    columns_to_process = [x for x in a_pivot.columns.to_list() if x not in ['Shares']]
    def do_subtract(df): #Encapsulating this code for better memory management - objects created inside will be discarded after return

        # This is a lookup table to make possible the numpy nansum operation below
        # Columns of this lookup table are: ['index', 'cik', 'start', 'end']
        earlier_period_indexes = df.index.to_frame(index=False).reset_index(drop=False) #Create the 'index' column

        # Integer comparisons are much faster than date and string comparisons, so we convert everything to integers
        for i in earlier_period_indexes.columns:
            earlier_period_indexes[i] = pd.to_numeric(earlier_period_indexes[i], downcast='integer')

        assert earlier_period_indexes['cik'].is_monotonic_increasing == True
        earlier_period_indexes = scan_periods_to_dict(earlier_period_indexes.to_numpy()) #numba dict of numpy index arrays

        # Subtracting earlier periods and modifying the dataset in numpy is much faster than in pandas
        numpy_df = df.loc[:,columns_to_process].to_numpy()
        for this_period in sorted(earlier_period_indexes.keys()):
            numpy_df[this_period] -= np.nansum(numpy_df[earlier_period_indexes[this_period]], axis=0)

        return numpy_df

    a_pivot[columns_to_process] = pd.DataFrame(data=do_subtract(a_pivot), columns=columns_to_process, index=a_pivot.index)
    # The processing in numpy above has cast quarter_count to float. We flip it back to int.
    # We also consider point-in-time data (quarter_count=0) to be for a single quarter (quarter_count=1)
    a_pivot['quarter_count'] = a_pivot['quarter_count'].round(0).astype(int).replace({0:1})
    # About 1% of records remain with a quarter_count > 1. Since we are building a quarterly dataframe, we will coerce
    # these records to a quarterly value.
    a_pivot = a_pivot.div(a_pivot['quarter_count'],axis='index').drop(columns='quarter_count')

    # Here we combine all data for a given end-date into one record. This unites all facts, regardles of their reporting
    # period - point-in-time or one quarter.
    a_pivot.index.rename({'end':'date'}, inplace=True)
    a_pivot = a_pivot.groupby(by=['cik','date']).mean()  #['cik','date'] is the new, sorted index

    # Sanitize the financial data according to business logic.
    a_pivot['Revenue'] = a_pivot['Revenue'].combine_first(a_pivot['COGS']+a_pivot['GrossProfit'])
    a_pivot['COGS'] = a_pivot['COGS'].combine_first(a_pivot['Revenue']-a_pivot['GrossProfit'])
    a_pivot['GrossProfit'] = a_pivot['GrossProfit'].combine_first(a_pivot['Revenue']-a_pivot['COGS'])
    a_pivot = a_pivot.dropna(thresh=3) #Remove the lines that have too little data
    a_pivot['NetCashFinancing'] = a_pivot['NetCashFinancing'].fillna(0.0)
    a_pivot['Liabilities'] = a_pivot['Assets']-a_pivot['Equity']

    def compute_ttm(df):
        """
        Here we compute trailing-twelve-month values out of per-quarter values.
        This can only be done on a cik-per-cik basis.
        """
        tags_to_ttm = ['Revenue', 'GrossProfit', 'NetCashOperating', 'NetCashFinancing', 'Earnings']
        ttm_columns = { i:f"{i}_ttm" for i in tags_to_ttm }
        ttm = df[tags_to_ttm].groupby(by='cik')
        ttm = ttm.rolling(window=pd.Timedelta(weeks=52), on=df.index.get_level_values('date'), min_periods=4).sum()
        return ttm.rename(columns=ttm_columns).droplevel(0)  #The grouping prepends a superfluous 'cik' index column
    a_pivot = pd.concat([a_pivot, compute_ttm(a_pivot)], axis='columns')

    if args.dump_intermediate_stages:
        a_pivot.to_csv(f'financials_{a_date:%Y-%m-%d}.csv.gz')

    # Drop all records per cik, leaving only the last one. This is the last known data for the given a_date and
    # the whole purpose of assembling the final dataset as a series of snapshots.
    a_pivot = a_pivot.reset_index(level='date',drop=False)
    a_pivot = a_pivot[~a_pivot.index.duplicated(keep='last')]
    a_pivot = a_pivot.set_index('date', append=True)  #re-introduce date as the right-most index column

    #Re-introduce the dei data, that is reported outside the date ranges of the us-gaap facts. Again, keep only the last known data.
    if not dei_facts.empty:
        dei_facts = merge_facts(dei_facts).dropna(axis='columns', how='all').dropna(axis='index', how='all').groupby(by='cik').last()
        a_pivot = a_pivot.drop(columns=dei_facts.columns.to_list()).merge(dei_facts,left_index=True,right_index=True,how='left')
        a_pivot['Shares'] = a_pivot['EntityShares'].combine_first(a_pivot['Shares'])

    # The final values will be in billions and rounded, except the headcount
    columns_to_round = a_pivot.columns.to_list()
    columns_to_round.remove('Employees')
    a_pivot[columns_to_round] = a_pivot[columns_to_round].map(lambda x:round(x/1e9, 6), na_action='ignore')

    # Add the snapshot date as the left-most index column
    a_pivot['snapshot'] = a_date
    a_pivot = a_pivot.set_index('snapshot', append=True)
    a_pivot = a_pivot.reorder_levels(['snapshot','cik','date'])

    print(f'Snapshot {a_date.date()}: {a_pivot.index.get_level_values('cik').nunique()} unique ciks processed.', file=sys.stderr)
    return a_pivot

def call_single_pivot(args):
    return process_single_pivot(*args)

# %%
def make_snapshots(companyfacts, args):
    # Keeping the index columns is vital to reduce the memory footprint. In its current form, the companyfacts
    # dataframe occupies about 1 GiB of RAM. If we do a .reset_index(drop=False), the size baloons to above 4GiB
    # Index columns are: filed,cik,end,start,fact
    # Value column is: val_final
    # These come largely from variable data_column_names in function batch_convert_json

    print('Pre-compiling numba functions...')  #Because they are used within the worker processes...
    scan_periods_to_dict(np.zeros(shape=(1,3)))
    print('Compile succeeded.')

    today = datetime.datetime.today().date()
    dates_to_compute = [ datetime.date(year,month,1) for year in list(range(2013,today.year+1)) for month in list(range(1,13)) ]
    dates_to_compute = [ i for i in dates_to_compute if i < today ]
    if args.partial_dataset:  # Only the first two, the last date and today
        dates_to_compute = [ dates_to_compute[i] for i in [0,1,-1] ]
    dates_to_compute.append(today)
    dates_to_compute = pd.to_datetime(dates_to_compute)
    print(f"Total number of dates to create snapshots for: {len(dates_to_compute)}")

    # We assume a cik no longer files with the SEC if its last filing date is at least one year earlier than the snapshot date.
    # We ignore these ciks to not clutter the output dataset with identical records for defunct ciks.
    # This provides a significant performance boost, too
    defunct_dates = companyfacts.groupby(by=['cik','filed']).first().index.to_frame(index=False).groupby('cik')['filed'].max() + pd.DateOffset(months=13)

    def make_pivot_parameters(a_date):
        active_ciks = defunct_dates[defunct_dates >= a_date].index.unique().tolist()
        a_pivot = companyfacts[companyfacts.index.isin(active_ciks, level='cik') & ( companyfacts.index.get_level_values('filed') < a_date )]
        return a_date, a_pivot, args

    pivot_parameters = map(make_pivot_parameters, dates_to_compute)

    dump_memory_usage('Start assembling snapshots')
    job_results = concurrent_map_fn(call_single_pivot, pivot_parameters)
    return pd.concat(job_results)

def post_process_dataset(snapshots):
    # There are several ciks, that have reported wrong data 1000 times larger than actual.
    # They apparently never submitted a correction. Also, some ciks have assets close to zero.
    # These errors disrupt downstream data usage and thus we remove the whole ciks.
    asset_outliers = snapshots.groupby(by='cik')['Assets'].aggregate(['min','max'])
    zero_asset_ciks = asset_outliers['max'].fillna(0.0).abs() 
    zero_asset_ciks = zero_asset_ciks[zero_asset_ciks < 1e-3].index.get_level_values('cik').unique().tolist()
    asset_outliers = (asset_outliers['min']/asset_outliers['max']).sort_values().fillna(0.0)
    asset_outliers = asset_outliers[asset_outliers<1e-3].index.get_level_values('cik').unique().tolist()
    ciks_to_exclude = set(zero_asset_ciks).union(asset_outliers)

    print(f'{len(ciks_to_exclude)/snapshots.index.get_level_values('cik').nunique():.1%} of ciks removed due to invalid Assets data')
    snapshots = snapshots[~snapshots.index.isin(ciks_to_exclude, level='cik')]

    # If cik-to-ticker mapping from the SEC is available, use it
    try:
        with open('company_tickers_exchange.json','r') as f:
            tickers = json.load(f)
        tickers = pd.DataFrame(columns=tickers['fields'], data=tickers['data'],dtype=str)
        tickers = tickers[tickers['exchange']!='OTC'].dropna() #leave only tickers traded on an exchange
        tickers['exchange'] = tickers['exchange'].str.upper().str.strip()
        tickers['cik'] = tickers['cik'].str.zfill(10)
        tickers['len'] = 4
        tickers.loc[tickers[tickers['ticker'].str.contains('-')].index,'len'] += 1
        tickers['ticker'] = tickers.apply(lambda row: str(row.ticker[:row.len]),axis='columns').str.rstrip().str.rstrip('-').str.upper()
        tickers = tickers.sort_values(by=['cik','exchange','ticker','len']).drop_duplicates(subset=['cik'],keep='first').set_index('cik').drop(columns=['len'])
    except:
        print(f'company_tickers_exchange.json not found in {os.getcwd()} or unreadable. Ticker columns will be empty.')
        tickers = None

    # We can now add the ticker and exchange columns, which are strings.
    snapshots = enrich_with_tickers(snapshots.copy(), tickers)

    # Enforce consistent column order across versions
    snapshots = snapshots[[i for i in export_schema if i not in list(snapshots.index.names)]] 

    return snapshots

# %%
def main(args, worker_pool=None):
    with redirect_stdout(sys.stderr):
        print('Python version:', sys.version)
        print('CLI switches:', args)
        print(f'Using {args.vCPUs} CPU(s), you should have at least {max(8,args.vCPUs*3)} GiB of RAM.')
        intermediate_stage_name = 'companyfacts.csv.gz'
        try:
            #If the intermediate stage is already present in the working directory, load it.
            companyfacts = pd.read_csv(intermediate_stage_name,
                                       index_col=['filed','cik','end','start','fact','quarter_count'],
                                       date_format='ISO8601',
                                       dtype={'cik':str})
            if companyfacts.empty:
                raise Exception
            else:
                print(f'{intermediate_stage_name} found and loaded. Skipping json processing.')
        except:
            companyfacts = make_companyfacts(args)
            if args.dump_intermediate_stages:
                print(f'Saving {intermediate_stage_name} as requested.')
                companyfacts.to_csv(intermediate_stage_name)
        companyfacts = make_snapshots(companyfacts.drop(columns=['val','corrected']), args) #val_final remains
        print('Finalizing dataset and exporting as CSV.')
        companyfacts = post_process_dataset(companyfacts)
    companyfacts.to_csv(sys.stdout, index=True, header=True, date_format='%Y-%m-%d', float_format='%f')

# %%
if __name__ == "__main__":
    cli_args = parse_arguments()
    if cli_args.max_jobs == 1:
        with cProfile.Profile(builtins=False) as pr:
            concurrent_map_fn = map
            main(cli_args)
            perf = pstats.Stats(pr, stream=sys.stderr).sort_stats('cumulative')
            perf.print_callees('parse_json_batched')
            perf.print_callees('process_single_pivot')
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=cli_args.vCPUs+1) as executor:
            # The buffersize parameter is critical, otherwise RAM runs out almost immediately with no benefit at all
            concurrent_map_fn = lambda fn, *fn_iter: executor.map(fn, *fn_iter, buffersize=cli_args.vCPUs)
            main(cli_args, executor)
    print('Done without errors.', file=sys.stderr)
