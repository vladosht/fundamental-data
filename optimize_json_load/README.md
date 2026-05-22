# JSON Parsing Optimization Project

Optimized parsing for a highly specialized JSON binary stream 

## Overview

The goal of this project is to create a highly optimized JSON parser for a specific JSON content. The premise is that the specific JSON binary stream contains too much data, which is irrelevant from the point of view of downstream consumers. Both performance and memory usage downstream can be significantly improved if the unwanted data is removed from the binary stream BEFORE it is json-parsed into a python object. Otherwise, the overhead of creating a complete in-memory Python object only to discard most of it, is significant.

The primary script, `main.py`, loads in-memory a large JSON binary from a file and then runs two different functions to parse it and the same function to clean it, timing each code path.

## Data reduction strategy

The superfluous data can be divided into two kinds:

### Unneeded facts

All facts consumed downstream are listed in the `known_facts` python list. Everything else is superfluous.

### Unneeded keys

The useful data is located at the bottom of the json hierarchy in the form of lists of objects. These objects have several unwanted keys, which are listed in the `unwanted_keys` python list. All others are needed.

## How to Run

To run the benchmark and see the performance comparison, execute the following command from the repository root:

```bash
python3 main.py
```

## Methodology

Two methods are compared:

### 1. Baseline Method (`parse_json_directly`)

This function parses the entire JSON binary object into a full Python dictionary. It relies entirely on the cleaning function `reduce_a_json_dict` to remove the unwanted keys.

### 2. Optimized Method (`parse_json_optimized`)

This function first uses the function `preprocess_json` on the binary stream to remove the unwanted data and then parses the json. The `preprocess_json` function sits into its own module. This module is the only target of all optimization efforts.

## Performance Results

A performance trace for function `preprocess_json` with individual callee timings is printed on standard error. The ratio between baseline and optimized time is printed on standard output. The ultimate goal of this project is this number to be as high as possible.
