# kathmandu-data-python
Prepares data required for visuallization section of C2M2 Kathmandu portal. It takes raw business and workforce survey data from C2M2 Kathmandu survey and performs univariate and bivariate analysis for charts visualization and also for maps section.

## Run Locally
You need to have access to raw data of C2M2 Kathmandu Survey to make use of this repo. When you have access to raw data you can prepare data for business and workforce survey using following commands.

Clone this repo
```bash
  git clone https://github.com/c2m2-asia/kathmandu-data-python.git
```

Open project directory
```bash
cd kathmandu-data-python
```
Install dependencies
```bash
  pip install -r requirements.txt
```

You need to have sql server(we have used postgres in our case) installed and running inorder to save data on sql server

To generate data for business survey
```bash
  python business.py
```

To generate data for workforce survey
```bash
  python workers.py
```
To generate sql dump form postgres server
```bash
`pg_dump -U <POSTGRES_USER> -h localhost <POSTRGRES_DATABASE> > <DUMP_FILE_NAME>
```
