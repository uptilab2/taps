#!/bin/bash

for tap in tap-*/setup.py
do
  python $tap install
done
