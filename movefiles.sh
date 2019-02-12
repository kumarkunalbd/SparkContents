#!/bin/bash
PATH_FROM='/Users/kumarkunal/Upgrad_materials/Course5-Mod7-SparkStream/Scripts_Shell/source_dir2'
PATH_TO='/Users/kumarkunal/Upgrad_materials/Course5-Mod7-SparkStream/Scripts_Shell/destination'

rm -rf $PATH_FROM/.DS_Store
rm -rf $PATH_TO/.DS_Store
ls -1 $PATH_FROM | while read -r serial
do
  cp $PATH_FROM/$serial $PATH_TO/$serial
  sleep 60
done
