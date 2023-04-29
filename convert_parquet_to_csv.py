import pandas as pd
import sys
import argparse
'''
This script is to convert parquet to csv file, by default it will change 
only first 50 lines, but if you give --number_lines 0 then it will convert 
the entire file
'''

if __name__ == '__main__':
    arg_parse = argparse.ArgumentParser(description='This Program is to convert parquet to csv format')
    arg_parse.add_argument('--parquet_file', type=str, help='full path of parquet file', required=True)
    arg_parse.add_argument('--csv_file', type=str, help='full path of csv file to save', required=True )
    arg_parse.add_argument('--number_lines', type=int, default=50, help='Number of rows to read, default 50 rows')
    args = arg_parse.parse_args()
    input_file = args.parquet_file
    output_file= args.csv_file
    nrows = args.number_lines
    print(f'nrows : {nrows}')
    if nrows == 0 :
        df = pd.read_parquet(input_file)
    else:
        df = pd.read_parquet(input_file).head(nrows)
    df.to_csv(output_file)
    