#!/bin/bash

/usr/bin/curl -s "$1" > docbin_full.json;
/usr/bin/curl -s "$2" > attribute_calculators.py;

/usr/local/bin/python run_ac.py "$3" "$4";
