# MPA + Global Fishing Effort Project

This repository primarily contains jupyter notebooks used to analyze
global fishing watch data:
https://globalfishingwatch.org/data-download/datasets/public-fishing-effort
in conjunction with mpa boundaries from mpatlas:
https://mpatlas.org/zones/
and:
https://www.protectedplanet.net/en


## Contents

util/ contains general purpose analysis tools written in python.

Some top level output csvs are saved in the outer directory as well.

2022 and 2021 contain snapshots of analysis work in the form of weekly
jupyter notebooks (that contained TODOs, notes, etc. as well as code in python).

A missing data/ folder is referenced in a lot of the code; the data sets are
too big to upload to github so this folder was entered into .gitignore to
avoid uploading it. Most of the preprocessing can be done by downloading
publically available data sets from the above sources and running methods
from the util folder.

Most work is done in python using the geopandas library.
One note: there's some wrangling you have to do with geopandas and pygeos
to avoid some apparently active bug in geopandas, see:
https://stackoverflow.com/questions/66128017/sjoin-contains-within-seems-to-be-returning-a-bunch-of-incorrect-rows

