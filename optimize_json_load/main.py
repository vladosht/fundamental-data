import sys, json, timeit, cProfile, pstats
from sec_json_tools import preprocess_json

# There is a very large number of XBRL tags present in the JSON file from the SEC.
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
unwanted_keys = ['accn','fp','fy','form','frame']

def reduce_a_json_dict(jdict):
    """
    We do data reduction here, because we do not need most of the json content
    """
    cik = str(jdict['cik']).zfill(10)  #A cik is always present
    j = list()

    def prune_enrich_dict(a_dict):
        return { k:v for k,v in a_dict.items() if k not in unwanted_keys} | {'cik': cik, 'fact': fact_path }

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
                j += list(map(prune_enrich_dict, fact_data))
                break
    return j

def main():
    with open('CIK0000001750.json','rb') as f:
        test_input = f.read()

    config_string = json.dumps((known_facts, unwanted_keys))

    def parse_json_directly():
        return reduce_a_json_dict(json.loads(test_input))

    def parse_json_optimized():
        return reduce_a_json_dict(json.loads(preprocess_json(test_input, config_string)))

    assert json.dumps(parse_json_optimized()) == json.dumps(parse_json_directly())

    repetition_count = 200
    t_direct, t_optimized = [ timeit.timeit(i, number=repetition_count) for i in [parse_json_directly, parse_json_optimized] ]

    print(f'Baseline time = {t_direct:.2f}, Optimized time = {t_optimized:.2f}, Total size of input = {repetition_count * sys.getsizeof(test_input)/2**20:.2f}MiB', file=sys.stderr)
    return t_direct/t_optimized

if __name__ == '__main__':
    print("Benchmark running on Python version", sys.version, file=sys.stderr)
    with cProfile.Profile() as pr:
        print(main())
        ps = pstats.Stats(pr, stream=sys.stderr)
    ps.print_callees('preprocess_json')
