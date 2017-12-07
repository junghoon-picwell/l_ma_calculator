# l_ma_calculator : Medicare Advantage Lambda-Hosted Calculator

## Purpose

This package is meant to provide a replacement for the usage of the MA Calculator contained here:
 
 `github.com/picwell/etl/tree/master/ma/calculator/ma_calculator` 

It serves a simple purpose: given a UID and the location of a plan benefits file (on S3),
it calculates Out-of-Pocket (OOP) costs for every plan for that user and saves them to a DynamoDB table.


## Usage

As this code is deployed as an AWS Lambda, it can be triggered through `boto3` or with an HTTP POST.

You call the `main()` function as the entry point.

To package this for use as a lambda, run `make package`.


```
usage: ma_calculator.py 


positional arguments:
  creds_file  credsfile containing 1 JSobj of `email` & `password`

optional arguments:
  -h, --help  show this help message and exit
```

