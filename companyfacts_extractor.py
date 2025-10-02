#!/usr/bin/env python3
# %%
import datetime
import requests
import io
import zipfile
import pandas as pd
import numpy as np
import json
import multiprocessing as mp

# %%
# If you happen to use this program, please change the user-agent string, because if too many people start using
# the same user-agent, the SEC will finally block it due to excessive usage
sec_headers = {'User-Agent':'Your Name your.email.here@example.org','Accept-Encoding':'gzip, deflate'}
today = datetime.datetime.today().date()
dates_to_compute = [ datetime.date(year,month,1) for year in list(range(2012,today.year+1)) for month in list(range(1,13)) ]
dates_to_compute = [ i for i in dates_to_compute if i < today ]
#dates_to_compute = dates_to_compute[:1]  #Uncomment this for test purposes
dates_to_compute.append(today)
print("Total number of dates to create snapshots for:", len(dates_to_compute))
export_additional_datasets = False

# %%
if __name__ == '__main__':
    mp.set_start_method('fork')
print('{} CPUs available'.format(mp.cpu_count()))
#This script has been tested on a c2d-highmem-32 (32 vCPUs, 256 GB Memory) VM instance, where it runs for about 15 minutes with peak RAM usage of about 80%
#In google cloud the above is the vCPU count. The actual CPU cores are half that. This also uses much less RAM
max_jobs = mp.cpu_count() // 2 + 1

# %%
r = requests.get('https://www.sec.gov/files/company_tickers_exchange.json', headers = sec_headers)
r.raise_for_status()
r = r.json()
tickers = pd.DataFrame(columns=r['fields'],data=r['data'],dtype=str)
tickers = tickers[tickers['exchange']!='OTC'].dropna() #leave only tickers traded on an exchange
tickers['exchange'] = tickers['exchange'].str.upper().str.strip()
tickers['cik'] = tickers['cik'].str.zfill(10)
tickers['len'] = 4
tickers.loc[tickers[tickers['ticker'].str.contains('-')].index,'len'] += 1
tickers['ticker'] = tickers.apply(lambda row: str(row.ticker[:row.len]),axis='columns').str.rstrip().str.rstrip('-').str.upper()
tickers = tickers.sort_values(by=['cik','exchange','ticker']).drop_duplicates(subset=['cik'],keep='last').set_index('cik').drop(columns=['len'])

# %%
# If you download the .zip file manually and put it in the same directory as this script,
# we can use that to avoid repeated downloads of the same file.
zip_filename = 'companyfacts.zip'
companyfacts_df_keys = ['cik','schema','tag','end','start']
try:
    with open(zip_filename, mode='rb') as f:
        print(f"{zip_filename} found locally, loading...")
        r = f.read()
except:
    print(f"{zip_filename} unavailable, downloading from SEC...")
    r = requests.get(f"https://www.sec.gov/Archives/edgar/daily-index/xbrl/{zip_filename}", headers = sec_headers)
    r.raise_for_status()
    r = r.content
    print('{} gigibytes retrieved'.format(len(r)/2**30))

# %%
source_zip = zipfile.ZipFile(io.BytesIO(r), mode='r')
all_files = source_zip.namelist()
print('{} files in archive'.format(len(all_files)))

# %%
# We do data reduction here. There is a very large number of XBRL tags present in the zip file from the SEC.
# We are only interested in a few of them, that will be used to construct the end results.
def deconstruct_json(j):
    try:
        this_cik = str(j['cik']).zfill(10)
    except:
        return

    known_facts = [
        'Revenues',
        'SalesRevenueNet',
        'SalesRevenueGoodsNet',
        'SalesRevenueServicesNet',
        'OtherSalesRevenueNet',
        'RevenueFromContractWithCustomerExcludingAssessedTax',
        'RevenueFromContractWithCustomerIncludingAssessedTax',
        'InterestAndDividendIncomeOperating',
        'InterestAndFeeIncomeLoansAndLeases',
        'NoninterestIncome',
        'OtherIncome',
        'CostOfRevenue',
        'CostOfGoodsAndServicesSold',
        'CostOfGoodsSold',
        'CostOfServices',
        'GrossProfit',
        'Assets',
        'LiabilitiesAndStockholdersEquity',
        'AssetsCurrent',
        'AssetsNoncurrent',
        'StockholdersEquity',
        'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
        'NetCashProvidedByUsedInOperatingActivities',
        'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
        'CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations',
        'NetCashProvidedByUsedInFinancingActivities',
        'NetCashProvidedByUsedInFinancingActivitiesContinuingOperations',
        'NetIncomeLoss',
        'ProfitLoss',
        'NetIncomeLossAttributableToNoncontrollingInterest',
        'NetIncomeLossAvailableToCommonStockholdersBasic',
        'WeightedAverageNumberOfShareOutstandingBasicAndDiluted',
        'WeightedAverageNumberOfDilutedSharesOutstanding',
        'WeightedAverageNumberOfSharesOutstandingBasic'
    ]

    try:
        dei_facts = list(j['facts']['dei'].keys())
    except:
        dei_facts = []

    all_facts = []
    for a_fact in known_facts + dei_facts:
        try:
            unit = 'shares' if 'Share' in a_fact else 'USD'
            schema = 'dei' if a_fact in dei_facts else 'us-gaap'
            a_fact_table = j['facts'][schema][a_fact]['units'][unit] if a_fact in dei_facts else j['facts'][schema][a_fact]['units'][unit]
            for i in range(0,len(a_fact_table)):
                for unwanted_column in ['accn','fy','fp','form','frame']:
                    a_fact_table[i].pop(unwanted_column,None)
            a_fact_table = [ {'cik':this_cik,'schema':schema,'tag':a_fact, **a_tab_line } for a_tab_line in a_fact_table]
            all_facts += a_fact_table
        except:
            pass
    return pd.DataFrame(all_facts) if all_facts else pd.DataFrame()

# %%
def convert_a_file(a_file):
    try:
        with source_zip.open(a_file) as f:
            jfile = json.load(f)
    except:
        print("Error during opening of:", a_file)
        return []
    try:
        df = deconstruct_json(jfile)
    except:
        print("Error during processing of:", a_file)
        return []
    return df

# %%
if __name__ == '__main__':
    with mp.Pool(processes=max_jobs) as p:
        companyfacts = list(p.map(convert_a_file,all_files))
print('A total of {} processed json files.'.format(len(companyfacts)))
#Tell the garbage collector to remove > 1GB from memory
del source_zip
del r

# %%
def make_companyfacts_df(list_of_processed_files):
    df = pd.concat(list_of_processed_files)
    df['cik'] = df['cik'].astype(str).str.zfill(10)
    df['schema'] = df['schema'].astype(str)
    df['tag'] = df['tag'].astype(str)
    df['start'] = df['start'].combine_first(df['end'])
    df['end'] = pd.to_datetime(df['end'],yearfirst=True,errors='coerce').dt.date
    df['start'] = pd.to_datetime(df['start'],yearfirst=True,errors='coerce').dt.date
    df['filed'] = pd.to_datetime(df['filed'],yearfirst=True,errors='coerce').dt.date
    df['val'] = pd.to_numeric(df['val'],errors='coerce').astype(float)
    # Many companies have submitted wrong or invalid values during the years. Most of these invalid values have been corrected eventually
    # with later filings. For each cik/fact/period combination, we determine the final available filing, so that the latest and most correct
    # value is used.
    print(len(df[df.isna().any(axis='columns')]),"rows removed from the companyfacts dataset due to invalid data")
    df = df.dropna()
    final_values = df.sort_values(by=companyfacts_df_keys + ['filed','val']).drop_duplicates(subset=companyfacts_df_keys,keep='last')
    df = pd.merge(df,final_values,how='left',on=companyfacts_df_keys,suffixes=(None, '_final'))
    df['corrected'] = df['val'] != df['val_final']
    df = df.sort_values(by=companyfacts_df_keys + ['filed','val_final']).reset_index(drop=True)
    return df
companyfacts = make_companyfacts_df(companyfacts)

# %%
def save_companyfacts(df, filename):
    df.to_csv(filename, index=False)
    print(f"{len(df['cik'].unique())} unique ciks fetched and saved to {filename}")

if __name__ == '__main__' and export_additional_datasets:
    print("Saving companyfacts dataset in a background process...")
    mp.Process(target=save_companyfacts, args=(companyfacts, "companyfacts.csv.gz")).start()

# %%
key_fields = ['cik','start','end']

# %%
# Here we read the entire companyfacts dataframe and remove all data, filed after the given cut-off date.
# This is the main business logic of this dataset, which views the SEC data as a series of snapshots,
# which contain the data, known to the markets, at a given date. The remaining data in the form of XBRL tags
# is combined into new columns, which carry the final meaning of the business data.
def create_pivot(a_date):
    pivot = companyfacts[companyfacts['filed'] < a_date]
    pivot = pivot.drop_duplicates(subset=companyfacts_df_keys,keep='last')
    pivot = pd.pivot_table(pivot,values='val_final', index=key_fields, columns='tag', aggfunc='sum').sort_index()

    #Some tags were introduced later during the years and for earlier snapshots we must add them manually to avoid KeyErrors
    for i in ['RevenueFromContractWithCustomerExcludingAssessedTax',
              'RevenueFromContractWithCustomerIncludingAssessedTax',
              'WeightedAverageNumberOfShareOutstandingBasicAndDiluted',
              'EntityNumberOfEmployees'
            ]:
        if i not in pivot.columns:
            pivot[i] = np.NaN

    def convert_to_column(df):
        lv_name = df.name
        df = df.reset_index().set_index(key_fields)
        df = df.rename(columns={df.columns[0]:lv_name})
        return df

    revenue = pivot['Revenues'].combine_first(
        pivot['SalesRevenueNet']).combine_first(
        pivot[['SalesRevenueGoodsNet',
               'SalesRevenueServicesNet',
               'OtherSalesRevenueNet']].sum(min_count=1,axis='columns')).combine_first(
        pivot['RevenueFromContractWithCustomerExcludingAssessedTax']).combine_first(
        pivot['RevenueFromContractWithCustomerIncludingAssessedTax']).combine_first(
        pivot[['InterestAndDividendIncomeOperating',
               'InterestAndFeeIncomeLoansAndLeases',
               'NoninterestIncome',
               'OtherIncome']].sum(min_count=1,axis='columns'))
    revenue.name = 'Revenue'
    revenue = convert_to_column(revenue)
    
    cogs = pivot['CostOfRevenue'].combine_first(
        pivot['CostOfGoodsAndServicesSold']).combine_first(
        pivot[['CostOfGoodsSold',
               'CostOfServices']].sum(min_count=1,axis='columns'))
    cogs.name = 'COGS'
    cogs = convert_to_column(cogs)
    
    gross_profit = pivot['GrossProfit']
    gross_profit.name = 'GrossProfit'
    gross_profit = convert_to_column(gross_profit)
    
    assets = pivot['Assets'].combine_first(
        pivot['LiabilitiesAndStockholdersEquity']).combine_first(
        pivot[['AssetsCurrent',
               'AssetsNoncurrent'
        ]].sum(min_count=1,axis='columns'))
    assets.name = 'Assets'
    assets = convert_to_column(assets)
    
    equity = pivot['StockholdersEquity'].combine_first(
        pivot['StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'])
    equity.name = 'Equity'
    equity = convert_to_column(equity)
        
    net_cash_operating = pivot['NetCashProvidedByUsedInOperatingActivities'].combine_first(
        pivot[['NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
               'CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations'
        ]].sum(min_count=1,axis='columns'))
    net_cash_operating.name = 'NetCashOperating'
    net_cash_operating = convert_to_column(net_cash_operating)
    
    net_cash_financing = pivot['NetCashProvidedByUsedInFinancingActivities'].combine_first(
        pivot['NetCashProvidedByUsedInFinancingActivitiesContinuingOperations'])
    net_cash_financing.name = 'NetCashFinancing'
    net_cash_financing = convert_to_column(net_cash_financing)

    earnings = pivot['NetIncomeLoss'].combine_first(
        pivot['ProfitLoss']).combine_first(
        pivot['NetIncomeLossAttributableToNoncontrollingInterest']).combine_first(
        pivot['NetIncomeLossAvailableToCommonStockholdersBasic'])
    earnings.name = 'Earnings'
    earnings = convert_to_column(earnings)

    shares = pivot['WeightedAverageNumberOfShareOutstandingBasicAndDiluted'].combine_first(
        pivot['WeightedAverageNumberOfDilutedSharesOutstanding']).combine_first(
        pivot['WeightedAverageNumberOfSharesOutstandingBasic'])
    shares.name = 'Shares'
    shares = convert_to_column(shares)
    
    new_columns = [assets, revenue, cogs, gross_profit, equity, net_cash_operating, net_cash_financing, earnings, shares]
    new_columns = pd.concat(new_columns,axis='columns').reset_index(drop=False)
    new_columns = new_columns.sort_values(by=key_fields)

    df_dei = pivot.groupby(by='cik')[['EntityCommonStockSharesOutstanding','EntityPublicFloat','EntityNumberOfEmployees']].last().reset_index(drop=False)
    df_dei = df_dei.rename(columns={'EntityCommonStockSharesOutstanding':'EntityShares','EntityPublicFloat':'PublicFloat','EntityNumberOfEmployees':'Employees'})

    print(f"Pivot for date {a_date} created.")
    return a_date, new_columns, df_dei

# %%
# Here we mostly compute trailing-twelve-month values out of the per-quarter values. This can only be done on a 
# cik-per-cik basis.
def per_cik_conversions(a_group):
    a_cik, df = a_group

    tags_to_ttm = ['Revenue', 'GrossProfit', 'NetCashOperating', 'NetCashFinancing', 'Earnings']
    ttm = df[tags_to_ttm].rolling(window=4).sum().rename(columns={ i:f"{i}_ttm" for i in tags_to_ttm })
    df = pd.concat([df,ttm],axis='columns')

    try:
        a_ticker = tickers.loc[a_cik]
    except:
        a_ticker = None
    
    df['ticker'] = a_ticker['ticker'] if a_ticker is not None else ''
    df['exchange'] = a_ticker['exchange'] if a_ticker is not None else ''
    
    return df

# %%
def process_single_pivot(input_pivot):
    a_date, a_pivot, df_dei = input_pivot

    column_indexes = [a_pivot.columns.get_loc(x) for x in a_pivot.columns.to_list() if x not in key_fields + ['Shares']]

    # The data in the SEC file has been submitted as relevant for different period lengths, usually from 1 quarter to 1 year.
    # Here we convert all data to per-quarter data by subtracting earlier periods from the later, larger ones, which contain them.
    preceding_rows_df = pd.merge(a_pivot[key_fields].reset_index(drop=False),
                                 a_pivot[key_fields].reset_index(drop=False),
                                how='outer',left_on='cik',right_on='cik',suffixes=('','_previous'))
    preceding_rows_condition = (preceding_rows_df['index_previous']<preceding_rows_df['index'])
    preceding_rows_condition = preceding_rows_condition & (preceding_rows_df['start_previous']>=preceding_rows_df['start'])
    preceding_rows_condition = preceding_rows_condition & (preceding_rows_df['end_previous']<=preceding_rows_df['end'])
    preceding_rows_df = preceding_rows_df[preceding_rows_condition]
    preceding_rows_df = preceding_rows_df.groupby(by='index')['index_previous'].apply(lambda x:x.to_list())
    preceding_rows_df = preceding_rows_df.sort_index()

    numpy_df = a_pivot.iloc[:,column_indexes].to_numpy()
    for i,preceding_row_indexes in preceding_rows_df.items():
        numpy_df[i] -= np.nansum(numpy_df[preceding_row_indexes],axis=0)
    a_pivot.iloc[:,column_indexes] = numpy_df

    a_pivot = a_pivot.rename(columns={'end':'date'}).sort_values(by=['date','start'])
    a_pivot = a_pivot.drop(columns=['start']).groupby(by=['cik','date']).mean()
    a_pivot['Revenue'] = a_pivot['Revenue'].combine_first(a_pivot['COGS']+a_pivot['GrossProfit'])
    a_pivot['COGS'] = a_pivot['COGS'].combine_first(a_pivot['Revenue']-a_pivot['GrossProfit'])
    a_pivot['GrossProfit'] = a_pivot['GrossProfit'].combine_first(a_pivot['Revenue']-a_pivot['COGS'])
    a_pivot = a_pivot.dropna(thresh=3) #Remove the lines that have too little data
    a_pivot['NetCashFinancing'] = a_pivot['NetCashFinancing'].fillna(0.0)
    a_pivot['Liabilities'] = a_pivot['Assets']-a_pivot['Equity']
    a_pivot = a_pivot.reset_index(drop=False)

    df_finished = pd.concat(list(map(per_cik_conversions,a_pivot.groupby(by='cik'))))

#   The final values will be in billions and rounded.
    columns_to_ignore_rounding = ['cik','date','ticker','exchange']
    for a_col in df_finished.columns.to_list() + ['EntityShares','PublicFloat']:
        try:
            if a_col in df_finished.columns and a_col not in columns_to_ignore_rounding:
                df_finished[a_col] = (df_finished[a_col] / 1e9).round(6)
            if a_col in df_dei.columns and a_col not in columns_to_ignore_rounding:
                df_dei[a_col] = (df_dei[a_col] / 1e9).round(6)
        except:
            print(f"Snapshot {a_date}: Error! Column {a_col} is not numeric!")

    df_finished = df_finished.sort_values(by=['cik','date'])
    df_finished['snapshot'] = a_date
    df_finished = df_finished.set_index(['snapshot','cik','date']).reset_index(drop=False)

    if a_date == today and export_additional_datasets:
        df_finished.drop(columns=['snapshot']).to_csv(f"financials.csv.gz",index=False)
        print('Snapshot for today: {} unique ciks exported to financials dataset'.format(len(df_finished.cik.unique())))
    
    df_finished = df_finished.drop_duplicates(subset=['snapshot','cik'],keep='last')
    df_finished = pd.merge(df_finished,df_dei,how='left',on='cik')
    df_finished['Shares'] = df_finished['EntityShares'].combine_first(df_finished['Shares'])
    df_finished = df_finished.drop(columns=['EntityShares'])

    print('Snapshot {}: {} unique ciks processed'.format(a_date, len(df_finished.cik.unique())))
    return df_finished

# %%
if __name__ == '__main__':
    print("Creating pivots...")
    with mp.Pool(processes=max_jobs) as p:
        all_pivots = list(p.map(create_pivot,dates_to_compute))

# %%
if __name__ == '__main__':
    print("Creating snapshots...")
    with mp.Pool(processes=max_jobs) as p:
        all_pivots = list(p.map(process_single_pivot,all_pivots))

print('Saving results as CSV...')
all_pivots = pd.concat(all_pivots)

# %%
if not all_pivots.empty:
    all_pivots.to_csv(f"snapshots.csv.gz",index=False)
    print('Done')
