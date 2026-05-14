#!/usr/bin/env python3
# %%
from contextlib import redirect_stdout
import os, sys, warnings, io, gc, argparse
import cProfile, pstats
import zipfile, json, itertools
import datetime, re
import pandas as pd
import numpy as np
from numba import njit
from joblib import Parallel, delayed, parallel_config, cpu_count
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
    args.vCPUs = cpu_count(only_physical_cores=False) if args.max_jobs < 1 else args.max_jobs

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

# %%
def dump_memory_usage(note=None,top_n=5):
    sizes = sorted([round(sys.getsizeof(obj)/2**30,3) for obj in gc.get_objects()],reverse=True)[:top_n]
    print(f"{note}:" if note is not None else '',
          f"{round(sum(sizes),1)}GiB used by the top {top_n} memory objects:", sizes, file=sys.stderr)

# %%
def reduce_a_json_dict(jdict):
    """
    We take the parsed json data and do data reduction here,
    because we do not need most of the content
    """
    # Basic sanity checks for the json contents. These checks fail with a KeyError
    cik = str(jdict['cik']).zfill(10)  #A cik must be present
    if not len(jdict['facts']['us-gaap'].keys()) >= 2:  #At least this many distinct facts must be present
        raise KeyError('No actual data found in json file.')

    j = list()
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
def batch_convert_json(list_of_json_files):
    df = pd.DataFrame() #Always return a DataFrame for easier post-procesing
    parsed_data = list()
    strip_unneeded = re.compile(r',"(accn|fp|form|frame)":"[^"]*"') # Data reduction => less RAM needed downstream. A modest speedup, too.
    for jfile in list_of_json_files:
        try:
            jfile = json.loads(strip_unneeded.sub('',jfile.decode('utf-8')))
        except json.JSONDecodeError as err:
            context_before = 20
            context_after = 20
            with redirect_stdout(sys.stderr):
                print('JSON error:',err)
                print('JSON file begins with:',jfile[:60]) #Dump the cik
                print('Error location:', err.doc[err.pos-context_before:err.pos+context_after])
                try:
                    index_in_original = jfile.index(err.doc[err.pos-context_before:err.pos])
                    print('Original context:', jfile[index_in_original:index_in_original+context_before+context_after])
                except:
                    pass
            sys.exit(1)
        try:
            parsed_data += reduce_a_json_dict(jfile)
        except KeyError:
            pass #A valid json file without financial data is not an error. Silently skipping...

    if parsed_data:
        df = pd.DataFrame(parsed_data)
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

# %%
def make_companyfacts(args, batch_size = 1000):
    # Here the list of files can be restricted for test and debugging purposes,
    # because the whole zip is large and takes time to process.
    debugging_slice = slice(None) if not args.partial_dataset else slice(0,2000)

    # Load the SEC zip file from standard input
    print("Reading standard input...")
    source_zip = zipfile.ZipFile(io.BytesIO(sys.stdin.buffer.read()), mode='r')
    files_to_parse = source_zip.namelist()[debugging_slice]
    print(f'Parsing {len(files_to_parse)} json files in batches of {batch_size}...')

    job_results = pd.DataFrame()

    current_batch = 0
    unzip_generator = (source_zip.read(a_file) for a_file in files_to_parse)
    for a_batch in itertools.batched(unzip_generator, batch_size):
        job_results = pd.concat([job_results] + Parallel()(delayed(batch_convert_json)([i]) for i in a_batch))
        current_batch += 1
        dump_memory_usage(f'Batch {current_batch} of json files completed')
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
@njit # Numba is the speed king :-)
def scan_periods(input_arr, output_arr):
    start, end = (1,2) #indexes of the period start and period end columns for both input and output arrays.
    return_length_only = len(output_arr.shape) <= 1
    actual_length = 0
    for i in np.arange(1, input_arr.shape[0]):
        this_period = input_arr[i]
        # Here we enforce the following logic:
        # index_earlier_period < index
        earlier_periods = input_arr[:i]
        # start_earlier_period >= start
        # end_earlier_period <= end
        earlier_periods = earlier_periods[(earlier_periods[:,start] >= this_period[start])&(earlier_periods[:,end] <= this_period[end])]
        remaining_length = earlier_periods.shape[0]
        if remaining_length < 1:
            continue
        # we do broadcasting, so that this period is in the second column (index 1)
        earlier_periods[:,1] = this_period[0]
        if not return_length_only:
            output_arr[actual_length:actual_length+remaining_length] = earlier_periods[:,:2]
        actual_length += remaining_length
    if return_length_only:
        output_arr[0] = actual_length
    return output_arr

# For all its raw speed, numba is best suited to work on numpy arrays that already exist and do not change shape.
# Therefore, here we implement a 2-pass logic: first pass to determine the shape of the output and a second
# pass to fill an output array with data.
def get_earlier_period_indexes(n):
    output_length = scan_periods(n, np.array([0]))[0]
    # We flip the output columns, so that the current period indexes are first. The downstream logic depends on this.
    return scan_periods(n, np.empty(shape=(output_length,2),dtype=n.dtype))[:,[1,0]] #<-the flip

# %%
def process_single_pivot(a_date, a_pivot, tickers_mapping, args):
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
    # Subtracting earlier periods and modifying the dataset in numpy is much faster than in pandas
    columns_to_process = [x for x in a_pivot.columns.to_list() if x not in ['Shares']]
    def do_subtract(df): #Encapsulating this code for better memory management - arrays created inside will be discarded after return
        # This is a lookup table to make possible the numpy nansum operation below
        # Columns of this lookup table are: ['index', 'cik', 'start', 'end']
        earlier_period_indexes = df.index.to_frame(index=False).reset_index(drop=False) #Create the 'index' column
        # Integer comparisons are much faster than date comparisons, so we convert the dates to integers
        earlier_period_indexes[['start','end']] = earlier_period_indexes[['start','end']].map(lambda x:x.toordinal())
        earlier_period_indexes = earlier_period_indexes.groupby(by='cik')[['index','start','end']].apply(
            lambda x:get_earlier_period_indexes(x.to_numpy()))
        earlier_period_indexes = np.concatenate(earlier_period_indexes.to_list())

        numpy_df = df.loc[:,columns_to_process].to_numpy()
        split_point_indexes = earlier_period_indexes[:,0] != np.roll(earlier_period_indexes[:,0], 1)
        split_point_indexes[0] = True
        split_point_indexes = np.nonzero(split_point_indexes)[0]
        for a_split in np.split(earlier_period_indexes, split_point_indexes)[1:]:
            numpy_df[a_split[0,0]] -= np.nansum(numpy_df[a_split[:,1]],axis=0)
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

    # After the rounding run we can now add the ticker and exchange columns, which are strings.
    a_pivot = enrich_with_tickers(a_pivot, tickers_mapping)

    # Add the snapshot date as the left-most index column
    a_pivot['snapshot'] = a_date
    a_pivot = a_pivot.set_index('snapshot', append=True)
    a_pivot = a_pivot.reorder_levels(['snapshot','cik','date'])

    a_pivot = a_pivot[[i for i in export_schema if i not in list(a_pivot.index.names)]]  #enforce consistent column order across versions

    ciks_nunique = a_pivot.index.get_level_values('cik').nunique()
    a_pivot = a_pivot.to_csv(index=True, header=False, date_format='%Y-%m-%d', float_format='%f')
    print(f'Snapshot {a_date.date()}: {ciks_nunique} unique ciks processed into a CSV string of {sys.getsizeof(a_pivot)/2**20:.2f}MB.', file=sys.stderr)
    return a_pivot

# %%
def make_snapshots(companyfacts, args):
    # Keeping the index columns is vital to reduce the memory footprint. In its current form, the companyfacts
    # dataframe occupies about 1 GiB of RAM. If we do a .reset_index(drop=False), the size baloons to above 4GiB
    # Index columns are: filed,cik,end,start,fact
    # Value column is: val_final
    # These come largely from variable data_column_names in function batch_convert_json

    today = datetime.datetime.today().date()
    dates_to_compute = [ datetime.date(year,month,1) for year in list(range(2013,today.year+1)) for month in list(range(1,13)) ]
    dates_to_compute = [ i for i in dates_to_compute if i < today ]
    if args.partial_dataset:  # Only first, last date and today
        dates_to_compute = [ dates_to_compute[i] for i in [0,-1] ]
    dates_to_compute.append(today)
    dates_to_compute = pd.to_datetime(dates_to_compute)
    print(f"Total number of dates to create snapshots for: {len(dates_to_compute)}", file=sys.stderr)

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
        print(f'company_tickers_exchange.json not found in {os.getcwd()} or unreadable. Ticker columns will be empty.', file=sys.stderr)
        tickers = None

    # We assume a cik no longer files with the SEC if its last filing date is at least one year earlier than the snapshot date.
    # We ignore these ciks to not clutter the output dataset with identical records for defunct ciks.
    # This provides a significant performance boost, too
    cik_filings = companyfacts.groupby(by=['cik','filed']).first().reset_index(drop=False)[['cik','filed']]
    defunct_dates = pd.to_datetime(cik_filings.groupby(by='cik')['filed'].max()) + pd.DateOffset(months=13)

    def make_single_pivot(a_date):
        active_ciks = set(defunct_dates[defunct_dates >= a_date].index.to_list())
        filing_dates = set(cik_filings[cik_filings['filed'] < a_date]['filed'].unique().tolist())
        a_pivot = companyfacts[companyfacts.index.isin(active_ciks, level='cik') & companyfacts.index.isin(filing_dates, level='filed')].copy()
        return a_date, a_pivot, tickers, args

    # We dump the results to stdout periodically, because otherwise RAM runs out pretty quickly.
    # This ugly workaround with batches is needed, because the legacy multiprocessing backend does not support return_as='generator'
    # By the way, re-using the worker pool by Prallel is possible, but for some reason this causes memory leaks that are very hard to track.
    # The implementation below turned out to be the most memory efficient.
    dump_memory_usage('Start assembling snapshots')
    print(','.join(export_schema)) #Print the CSV header
    for a_batch in itertools.batched(dates_to_compute, 2*args.vCPUs):
        print(''.join(Parallel()(delayed(process_single_pivot)(*make_single_pivot(i)) for i in a_batch)), end='')
        dump_memory_usage('A batch was completed')

# %%
def main(args):
    with redirect_stdout(sys.stderr):
        print('Python version:', sys.version)
        print('CLI switches:', args)
        print('Compiling numba functions...')
        if get_earlier_period_indexes(np.zeros(shape=(1,1))).size >= 0:
            print('Compile succeeded.')
        print(f'Using {args.vCPUs} CPU(s), you should have at least {max(2,args.vCPUs)*4} GiB of RAM.')
        dump_memory_usage('Program start')
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
    make_snapshots(companyfacts.drop(columns=['val','corrected']), args) #val_final remains

# %%
if __name__ == "__main__":
    cli_args = parse_arguments()
#   In my experience, the legacy backend is quite solid. The default 'loky' backend keeps throwing
#   warnings about memory leaks of unknown origin and is too picky as it tries to pickle function arguments and global variables
#   that cannot be pickled (ZipFile objects are one example).
#   The threading backend works essentially with only one CPU core because of the python GIL and for now I cannot think
#   of any way to make the logic in this program thread-safe without making an unmaintainable mess out of it.
    with parallel_config(backend='multiprocessing', n_jobs=cli_args.max_jobs):
        if cli_args.max_jobs == 1:
            with cProfile.Profile(builtins=False) as pr:
                main(cli_args)
                pstats.Stats(pr, stream=sys.stderr).sort_stats('cumulative').print_callees('batch_convert_json')
                pstats.Stats(pr, stream=sys.stderr).sort_stats('cumulative').print_callees('process_single_pivot')
        else:
            main(cli_args)
    print('Done without errors.', file=sys.stderr)
