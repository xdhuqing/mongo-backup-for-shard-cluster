#!/bin/bash

cd `dirname $0`
path=$(pwd)"/"

tar -zxvf pymongo-3.2.2.tar.gz

cd ${path}"pymongo-3.2.2"
python setup.py install
