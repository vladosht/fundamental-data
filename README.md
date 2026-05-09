# fundamental-data
Scripts to process and model fundamental financial data sourced from the US SEC

## Why this repository exists
The United States Securities and Exchange Commission (SEC) [provides](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) **nightly** builds of a bulk financial data file with the name **companyfacts.zip**. This file is a treasure trove for anyone interested in Business and Finance, as it contains all financial data points which all public companies have reported to the SEC since 2010 until literally yesterday. The file is free for anyone to download and use.

Usually the multitude of alternative, non-government sources of such information charge significant subscription fees. The reason for this is that the SEC data is difficult to work with. The zip file contains the raw XBRL tags, and as with any real-world data, it is impressively messy. It requires a lot of effort to clean, validate and aggregate.

The purpose of the companyfacts_extractor.py program in this repository is to automate this process as much as possible. It takes the as-downloaded zip file and outputs a CSV file that is much easier to understand and use.

DISCLAIMER: Please pay attention to the license of this repository, which is the GNU General Public License version 3. It says literally that "This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY".

That said, if you spot a bug or want to contribute, feel free to reach out or send pull requests. All forms of collaboration are welcome.

The *jupyter notebooks* here were originally developed on [Kaggle](https://www.kaggle.com/vladosht/code) and are synced here as a backup. They use the output of the program, which is available as a frequently updated [dataset](https://www.kaggle.com/datasets/vladosht/fundamental-data-from-sec-xbrl-companyfacts-zip). So, if you just want to use it, log in to Kaggle, add the dataset to your notebook and you are good to go.

## Install and run
1. Clone this repository
2. Download the companyfacts.zip to its directory
3. Optionally download there the [company_tickers_exchange.json](https://www.sec.gov/files/company_tickers_exchange.json) file, if you want the output CSV to contain the ticker symbols of the public companies. This file is also provided by the SEC.
4. As this is a python project, check that you have python installed with `python3 --version`. The program needs at least version 3.9
5. For the version displayed, make sure that all packages listed in the requirements.txt are installed and importable.
6. Make sure you have at least 4GiB of RAM available per CPU core/thread, but no less than 8GiB in total.
7. Change to the repository directory with cd and read the online help of the program with `./companyfacts_extractor.py --help`. If you can, then your python environment is set up accordingly. 
8. Run `./companyfacts_extractor.py <companyfacts.zip | gzip >snapshots.csv.gz` and wait until the program finishes and displays "Done without errors". This will take a while. The next section provides some timings.
9. You can now directly import the CSV in pandas with the following statement:
`snapshots = pd.read_csv('snapshots.csv.gz', index_col=['snapshot','cik','date'], date_format='ISO8601', dtype={'cik':str})`

## Side notes
*companyfacts_extractor.py* was developed and tested in a Google Cloud Platform Virtual Machine of type n4d-standard-2  
It is a 2vCPU, 8GiB RAM amd64 instance, running Debian GNU/Linux 13 (trixie).  
The sysbench (v1.0.20) score of the instance, obtained with "sysbench --threads=\$(nproc) cpu run", is 5375.75  
In this instance, the program runs to completion within 20 min. This amounts to a cost of about $0.04 per full dataset run.  
The program is a native command line tool which is quite verbose, but only on stderr. Standard output is reserved only for the CSV stream. Therefore, it should be readily usable by AI agents.
