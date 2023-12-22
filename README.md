# BigDataExercises

Repository for Practical Work in the UPM Master's in Data Science course on Big Data.


## Data

Alocatted in [URL](https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/HG7NV7/EIR0RA&version=1.0)

```bash
wget https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/EIR0RA
wget https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/QJKL3I


bzip2 -d "file.bz2" 
```

## Launch App


```bash
spark-submit --num-executors 4 app/app.py --data_path=./ds/ --params_path=./app/model_params.json --out_path=./output
```

