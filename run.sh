#!/bin/bash

nvim essa.py 
clear 
.venv/bin/python3 essa.py --date-from 2024-01 --date-to 2026-12 $1 $2 | less -R 
echo -e "TEMPLATE: \n" 
tail -n +2 templates/$1.csv | .venv/bin/csvlook 
echo -e "\n\n RAPORT: \n" 
tail -n +2 raports/$2.csv | .venv/bin/csvlook
