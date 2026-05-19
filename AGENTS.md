# General information about this repository

This repository contains an Extract-Transform-Load application designed to run on a Debian GNU/Linux (trixie) system. The core logic is implemented in python and the orchestration is implemented with shell scripts.  
The application downloads a publicly-available bulk financial data file `companyfacts.zip` from the website of the United States Securities and Exchange Commission, performs data reduction, data transformation and saves the result in a Google Cloud Storage bucket. This .zip archive contains around twenty thousand JSON files, most of which have an uncompressed size of around three megabytes. The .zip archive is updated nightly by the SEC and tends to grow by around a hundred megabytes per year. For more business and technical context please consult the `README.md` file.  
The `*.ipynb` files are Jupyter Notebooks which serve as examples how to use the resulting dataset and are not part of the application itself.  

# Mandatory rules you must follow at all times

- **NEVER** read or use any files mentioned in the special files `.dockerignore` or `.gitignore`
