#!/usr/bin/env python3
"""Collect small per-Run summaries without pooling/averaging independent Runs."""
import argparse
import csv
import sys
from pathlib import Path

TABLES = ('hop_quality.csv','hop_timing.csv','message_summary.csv',
          'path_status_summary.csv','control_summary.csv')

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('inputs',nargs='+',type=Path,help='extractor output directories (shell globs accepted)')
    p.add_argument('--out',required=True,type=Path)
    a=p.parse_args()
    if a.out.exists() and any(a.out.iterdir()):
        p.error('The output directory is not empty.')
    a.out.mkdir(parents=True,exist_ok=True)
    directories=[]
    seen=set()
    for directory in a.inputs:
        directory=directory.resolve()
        if directory in seen:continue
        if not (directory/'manifest.json').is_file():
            p.error(f'Not an extractor output directory: {directory}')
        seen.add(directory);directories.append(directory)
    for name in TABLES:
        writer=None
        header=None
        with (a.out/name).open('w',encoding='utf-8-sig',newline='') as dst:
            for directory in directories:
                with (directory/name).open(encoding='utf-8-sig',newline='') as src:
                    reader=csv.DictReader(src)
                    if header is None:
                        header=reader.fieldnames
                        writer=csv.DictWriter(dst,fieldnames=['source_summary_directory',*header])
                        writer.writeheader()
                    elif header!=reader.fieldnames:
                        p.error(f'Schema versions differ: {directory/name}')
                    for row in reader:
                        writer.writerow({'source_summary_directory':str(directory),**row})
    print(f'{len(directories)} results combined row by row: {a.out}')
    print('No Run averages or weighted averages were calculated. Check run_id for duplicate use of the same Run.')

if __name__=='__main__':main()
