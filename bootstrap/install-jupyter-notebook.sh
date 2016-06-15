#!/bin/bash
set -x -e


#Installing iPython Notebook
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
cd /home/hadoop
sudo pip install virtualenv
mkdir Jupyter
cd Jupyter
/usr/bin/virtualenv -p /usr/bin/python2.7 venv
source venv/bin/activate

#Install jupyter and dependency
pip install --upgrade pip
pip install jupyter requests numpy matplotlib s3cmd

#Create profile
# jupyter profile create default
jupyter notebook --generate-config

#Run on master /slave based on configuration

echo "c = get_config()" >  /home/hadoop/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.ip = '*'" >>  /home/hadoop/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.open_browser = False"  >>  /home/hadoop/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.port = 8192" >>  /home/hadoop/.jupyter/jupyter_notebook_config.py

fi
