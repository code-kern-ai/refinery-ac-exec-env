#!/bin/bash

/usr/bin/curl -s "$1" > docbin_full.json;
/usr/bin/curl -s "$2" > attribute_calculators.py;
/usr/bin/curl -s "$3" > knowledge.py;

/usr/local/bin/python -u run_ac.py "$4" "$5" "$6";
