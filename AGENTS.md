# Context information and instructions

## General information about this repository

This repository contains an Extract-Transform-Load application designed to run on a Debian GNU/Linux (trixie) system. The core logic is implemented in python and the orchestration is implemented with shell scripts.  
The application downloads a publicly-available bulk financial data file `companyfacts.zip` from the website of the United States Securities and Exchange Commission, performs data reduction, data transformation and saves the result in a Google Cloud Storage bucket. This .zip archive contains around twenty thousand JSON files, most of which have an uncompressed size of around three megabytes. The .zip archive is updated nightly by the SEC and tends to grow by around a hundred megabytes per year. For more business and technical context please consult the `README.md` file.

## Design choices

The way the application is currently implemented is a consequence of design choices, shaped by goals. This section is intended to state them explicitly.

### Goals
The goals of the application architecture, ordered by priority, are:

1. **Versatility** - the core business functionality must be suitable for the widest variety of use cases possible. This is the most important goal and when it conflicts with other goals, versatility must be preferred.
2. **Lowest cost per output** - the application must be as fast as possible **AND** use as little RAM as possible. Compute work and memory size both incur costs in cloud environments. In local environments, compute work incurs energy costs. So both resources must be minimized, but when there is a trade-off between the two, less compute work should be preferred.

### Application structure

This section describes the design considerations that led to the current repository contents. In general terms, this repository is conceptually structured in three layers:

1. The most general-use core, which makes as few assumptions about its environment as possible.
2. A specialized wrapper to simplify the most common usage as a stand-alone program running on a local machine or in a cloud-based virtual machine.
3. A more specialized wrapper which allows containerization targeting Google Cloud Run.

This allows out-of-the-box usage possibilities according to cost or convenience considerations of the end user, as well as providing samples for other, yet unknown, use cases.

#### The project core: `companyfacts_extractor.py`

This script is monolithic by design and, besides the standard library, it has only a few dependencies. This makes it more likely to be useful without modification in environments and workflows it was not explicitly meant for. The specific design choices here are:
- **A file system is not required** - the script reads the input .zip from stdin and writes CSV text to stdout regardless of where the .zip file came from or where the CSV result is going to. Unfortunately, this mandates that the .zip file is always loaded completely into memory, because the .zip format contains important housekeeping information at the *end* of the file. Without this information, accessing the individual JSON files inside is impossible.
- The `company_tickers_exchange.json` exception - this file is publicly available from the SEC and is used to enrich the dataset with stock exchange ticker symbols for each entity. The core script expects this file to be present in its working directory. This file however is **STRICTLY** optional. If it is not accessible for whatever reason, the main logic issues a warning and continues without it, resulting in a dataset which only contains SEC cik numbers and no ticker symbols.  
The reason why this logic was incorporated anyway, is that if ticker symbols are present in the output dataset, the downstream data usage and modeling tends to be more straightforward and with less dependencies, which is a worthwhile benefit.
- **No internet access** - the script never attempts to use the internet or any other network protocol because it relies on stdin and stdout.
- **NEVER interactive** - once started, the script either runs to completion or fails. User input is NEVER requested. All status messages for the user are dumped to stderr, so that stdout is exclusively used for the output CSV text.

#### Wrapper for local PC execution or inside Virtual Machines in the cloud.

The `run.sh` orchestrates download of the necessary files from the SEC and the upload of the resulting dataset to a Google Cloud Storage bucket.
- The **download** uses the ubiquitous system utility `curl`. This was intentional in order to remove a python dependency to the `requests` module.
- The **upload** uses the provided `upload_results.py`, which was created to remove a dependency to the gcloud CLI, which could be problematic to provide in some environments. A dependency to the Google python SDK was introduced instead, which is easy to install with `pip`. The `upload_results.py` also ingests standard input completely in memory, but this was intentional, in order to keep the script as simple as possible. As it operates on the compressed CSV output of the core logic, which does not exceed a hundred megabytes, this is not an issue and the achieved simplification is more important.
- **Configuration** of both download and upload is implemented with two environment variables that the user may set in their `.bashrc` file, or using their OS'es preferred mechanism for environment variables. The choice to use environment variables was made, because this repository is publicly hosted on GitHub, so no specific configuration settings must be stored in repository files. In particular, the SEC-mandated user-agent string for download and GCS bucket name for upload are not supposed to be public, although their sensitivity level is not very high. Using environment variables is also compatible with execution in containerized form (see below).

#### Wrapper for Google Cloud Run

The provided Dockerfile and `deploy.sh` handle the generation of a Google Cloud Run job (not a service!) directly from inside the repository. The `run.sh` script is used as the entry point for the job. For all this to work, a suitably configured and authenticated gcloud CLI is a requirement. The environment variables, which `run.sh` expects to be present, are used to initially set up the job's environment. It can later be adjusted, for example directly from the Google Cloud Console Web UI.

#### The `*.ipynb` files

These are Jupyter Notebooks which serve as examples how to use the resulting dataset and are not part of the application itself. They may also serve as test cases for the correctness of the data, therefore you MUST NEVER read or analyze them. Otherwise you may make decisions which optimize the application logic specifically for these particular Jupyter Notebooks, leading to poor results in other, unrelated, use cases. This MUST be avoided as it conflicts with the top priority goal of versatility.

## Mandatory rules you must follow at all times

- **NEVER** read or use any files mentioned in the special files `.dockerignore` or `.gitignore`
