import os, subprocess, shutil, pathlib, numpy as np, pandas as pd
import google.cloud.bigquery, google.cloud.bigquery_storage
## requires MDB Tools: https://github.com/mdbtools/mdbtools
## Only tested in GCP Vertex AI using BigQuery

ROOT_PATH  = pathlib.Path('/home/jupyter/peer_aspirant/data')
DATASET    = 'cmat-315920.peer_aspirant2'
cred, proj = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
bqclient   = google.cloud.bigquery.Client(credentials=cred, project=proj)
try:
    bqclient.get_dataset(DATASET) 
except:
    bqclient.create_dataset(DATASET)

## refresh levels - cumulative ... refresh everything <= given value (refresh=4 does 1,2,3,4)
## 0 - None
## 1 - global joined data & common vars
## 2 - yearly joined data
## 3 - global vars
## 4 - yearly vars
## 5 - everything - redownload accdb & recreate each survey data table

concat_str = ' ... '
divide_str = '\n=======================================================================================\n'
def join_str(indents=1):
    tab = '    '
    return ',\n' + tab * indents

def rpt(msg=''):
    print(msg, end=divide_str)

def subquery(query, indents=1):
    s = join_str(indents)[1:]
    return query.strip().replace('\n', s)

def listify(x=None):
    if x is None:
        return []
    elif isinstance(x, pd.core.frame.DataFrame):
        x = x.to_dict('split')['data']
    elif isinstance(x, (np.ndarray, pd.Series)):
        x = x.tolist()
    elif isinstance(x, (list, tuple, set)):
        return list(x)
    else:
        return [x]

def prep(df):
    df.columns = [x.lower() for x in df.columns]
    return df
    
def run_query(query):
    res = bqclient.query(query).result()
    try:
        return prep(res.to_dataframe())
    except:
        return True

def get_schema(tbl):
    """Get schema of tbl"""
    t = bqclient.get_table(tbl)
    return t.schema
    
def get_cols(tbl):
    """Get list of columns of tbl"""
    return [s.name for s in get_schema(tbl)]

def read_table(tbl, cols='*', rows=999999999, start=0):
    query = f'select {", ".join(listify(cols))} from {tbl} limit {rows} offset {start}'
    return run_query(query)
    
def delete_table(tbl):
    query = f"drop table {tbl}"
    try:
        run_query(query)
    except google.api_core.exceptions.NotFound:
        pass

def load_table(tbl, df=None, query=None, file=None, overwrite=False, schema=None, preview_rows=0):
    """Load data into tbl either from a pandas dataframe, sql query, or local csv file"""
    if overwrite:
        delete_table(tbl)
    try:
        get_cols(tbl)
    except google.api_core.exceptions.NotFound:
        if df is not None:
            job = bqclient.load_table_from_dataframe(df, tbl).result()
        elif query is not None:
            conf = google.cloud.bigquery.QueryJobConfig(destination=tbl, write_disposition='write_append')
            job = bqclient.query(query, job_config=conf).result()
        elif file is not None:
            with open(file, mode='rb') as f:
                if schema:
                    job = bqclient.load_table_from_file(f, tbl, job_config=google.cloud.bigquery.LoadJobConfig(schema=schema)).result()
                else:
                    job = bqclient.load_table_from_file(f, tbl, job_config=google.cloud.bigquery.LoadJobConfig(autodetect=True)).result()
        else:
            raise Exception('at least one of df, query, or file must be specified')

def pad(year, l=2):
    return str(year)[-2:].rjust(l, '0')
        
def get_full_year(year):
    return '20' + pad(year)

def file_to_tbl(file):
    return f'{DATASET}.{file.stem}'


class Base():
    def __init__(self, file, name='base', refresh=False, **kwargs):
        self.name    = name
        self.file    = file
        self.tbl     = file_to_tbl(self.file)
        self.df      = None
        self.refresh = refresh
        if self.refresh:
            self.delete()

    def delete(self):
        delete_table(self.tbl)
        try:
            self.file.unlink()
        except FileNotFoundError:
            pass

    def exists(self, get=False):
        msg = [True, self.name]
        try:
            self.df.shape
            msg[1] += f'{concat_str}dataframe exists'
        except:
            if self.file.is_file():
                msg[1] += f'{concat_str}{self.file.name} exists'
                if get:
                    msg[1] += f'{concat_str}read_parquet{concat_str}'
                    try:
                        self.df = prep(pd.read_parquet(self.file))
                        msg[1] += f'success'
                    except:
                        msg[1] += f'fail'
            else:
                try:
                    get_cols(self.tbl)
                    msg[1] += f'{concat_str}{self.tbl} exists'
                    if get:
                        msg[1] += f'{concat_str}read{concat_str}'
                        try:
                            self.df = prep(read_table(self.tbl))
                            msg[1] += f'succeeded'
                        except:
                            msg[1] += f'failed'
                except:
                    msg = [False, f'{self.name} not found']
        return msg

    def get(self):
        return self.exists(True)

    def write(self):
        self.get()
        if self.file.is_file() is False:
            try:
                self.df.to_parquet(self.file)
            except:
                self.df.to_frame().to_parquet(self.file)
        load_table(tbl=self.tbl, df=self.df)


class Accdb(Base):
    def __init__(self, encoding='UTF-8', refresh=False, **kwargs):
        super().__init__(**kwargs)
        self.encoding = encoding
        self.xlsx = self.file.parent / f'{self.name}TablesDoc.xlsx'
        if refresh:
            shutil.rmtree(self.file.parent, ignore_errors=True)

    def get(self):
        """Download and unzip file from IPEDS if necessary"""
        try:
            self.schema
            msg = [True, '']
        except AttributeError:
            msg = self.exists()
            if msg[0] is False:
                path = self.file.parent
                path.mkdir(parents=True, exist_ok=True)
                os.chdir(path)
                for status in ['Final', 'Provisional']:
                    fn = self.file.stem
                    zip = path / f'{fn[0:5]}_{fn[5:9]}-{fn[9:11]}_{status}.zip'
                    if zip.is_file() is False:
                        url = f'https://nces.ed.gov/ipeds/tablefiles/zipfiles/{zip.name}'
                        msg[1] += f'{concat_str}trying {url}'
                        os.system(f'wget -q {url}')
                    if zip.is_file():
                        os.system(f'unzip -n {zip}')
                        msg[1] += f'{concat_str}success'
                        break
                    else:
                        msg[1] += f'{concat_str}fail'

            if self.exists()[0] is False:
                shutil.rmtree(path)
                raise Exception(msg[1])

            """Get dict of tables and their columns/data types. Inspired by pandas_access https://pypi.org/project/pandas/"""
            output = subprocess.check_output(['mdb-schema', self.file])
            lines = output.decode(self.encoding).splitlines()
            msg[1] += f'{concat_str}get schema'

            # Build schema dict
            self.schema = dict()
            for l in lines:
                a = l.strip().lower()
                if 'create table' in a:
                    # this line begins a new table
                    tbl = a.split("[")[1][:-1]
                    self.schema[tbl] = []
                elif (a == '') or (a[0] in ['', '-', '(', ')']):
                    # this line is irrelevant
                    pass
                elif a[0] == '[':
                    # this line is a column & data type
                    b = a.split('\t')
                    col = b[0] .replace('[','').replace(']','')
                    dt  = b[-1].replace(',','')
                    # convert from access data types to pandas data types
                    if 'integer' in dt or 'byte' in dt:
                        dt = 'integer'
                    elif 'double' in dt or 'numeric' in dt:
                        dt = 'float'
                    elif ('text' in dt) or ('link' in dt):
                        dt = 'string'
                    else:
                        raise Exception(f'Got unexpected dtype {dt}')
                    self.schema[tbl].append({'name':col, 'type': dt})
                else:
                    raise Exception(f'Got unexpected line {a}')
            msg[1] += f'{concat_str}success'
            self.schema = {tbl: sch for tbl, sch in self.schema.items() if 'unitid' in {col['name'] for col in sch} and 'mission' not in tbl}
            rpt(msg[1])
        return msg

    def read_table(self, t):
        """Read table from .accdb"""
        self.get()
        csv = t.file.with_suffix('.csv')
        cmd = f'mdb-export {self.file} {t.name.upper()} > {csv}'
        os.system(cmd)
        with open(csv, 'r') as f:
            lines = f.readlines()
        # I'm sure there's a prefered way using subprocess, but I was in a rush and used the older os.system 
        # output = subprocess.check_output(['mdb-export', self.file, t.name.upper()])
        # lines = output.decode(self.encoding).splitlines()
        with open(csv, 'w') as f:
             f.writelines(lines[1:])
        load_table(tbl=t.tbl, file=csv, schema=t.schema)
        msg = t.get()
        t.write()
        # csv.unlink()
        return msg


class IpedsYear():
    def __init__(self, refresh=1, year=2019, path=ROOT_PATH, **kwargs):
        self.refresh      = int(refresh)
        self.year         = year
        self.full_year    = get_full_year(self.year)
        self.name         = self.full_year
        self.path         = pathlib.Path(path)
        self.year_path    = self.path / f'{self.full_year}'
        self.raw_path     = self.year_path / 'raw'

        kwargs['name']    = f'{self.full_year}data'
        kwargs['file']    = self.year_path / f'{kwargs["name"]}.parquet'
        kwargs['refresh'] = self.refresh >= 2
        self.data         = Base(**kwargs)

        kwargs['name']    = f'{self.full_year}vars'
        kwargs['file']    = self.year_path / f'{kwargs["name"]}.parquet'
        kwargs['refresh'] = self.refresh >= 4
        self.vars         = Base(**kwargs)

        kwargs['name']    = f'IPEDS{self.full_year}{pad(self.year+1)}'
        kwargs['file']    = self.raw_path / f'{kwargs["name"]}.accdb'
        kwargs['refresh'] = self.refresh >= 5
        self.accdb        = Accdb(**kwargs)


    def get_vars(self):
        msg = self.vars.get()
        if msg[0] is False:
            self.accdb.get()
            msg[1] += f'{concat_str}loading {self.accdb.xlsx.name}'
            for sheet_name in [f'vartable{pad(self.year)}', f'varTable{pad(self.year)}']:
                msg[1] += f'{concat_str}sheet {sheet_name}'
                try:
                    self.vars.df = prep(pd.read_excel(self.accdb.xlsx, sheet_name=sheet_name))
                    msg[1] += f'{concat_str}success'
                    break
                except:
                    msg[1] += '{concat_str}fail'
            for c in ['tablename', 'survey', 'varname', 'imputationvar', 'format']:
                self.vars.df[c] = self.vars.df[c].str.lower()
            self.vars.df.insert(0, 'year', int(self.full_year))
        self.vars.write()
        rpt(msg[1])
        return msg


    def get_data(self):
        msg = self.data.get()
        if msg[0] is False:
            self.accdb.get()
            msg[1] += f'{concat_str}must get each table then combine'
            rpt(msg[1])
            self.tables = []
            kwargs = {'refresh': self.refresh >= 5}
            for tbl, schema in self.accdb.schema.items():
                kwargs['name'] = tbl
                kwargs['file'] = self.year_path / f'{kwargs["name"]}.parquet'
                t = Base(**kwargs)
                t.schema = schema
                sub_msg = t.get()
                if sub_msg[0] is False:
                    sub_msg[1] += f'{concat_str}loading from {self.accdb.name}'
                    rpt(sub_msg[1])
                    self.accdb.read_table(t)
                    del t.df
                    
                query = f'select distinct count(*) from {t.tbl} group by unitid'
                v = run_query(query).values
                try:
                    keep = v.max()==1
                except: # empty table
                    keep = False
                if keep:
                    self.tables.append(t)
                    sub_msg[1] += f'{concat_str}no duplicate unitids found'
                else: # skipping tables with duplicated unitid - we are not equipped to handle this yet
                    sub_msg[1] += f'{concat_str}table either empty or contains duplicate unitid{concat_str}skipping'
                rpt(sub_msg[1])

            msg[1] += f'{concat_str}combining tables'
            rpt(msg[1])
            
            j = '\n    '
            sel  = f'SELECT{j}unitid,{j}{self.full_year} AS year,'
            used = {'unitid', 'year'}#, 'line'}
            join = None
            for t in self.tables:
                if join is None:
                    join = f'FROM{j}{t.tbl} AS {t.name}'
                else:
                    join += f'\nFULL OUTER JOIN{j}{t.tbl} AS {t.name}\nUSING{j}(unitid)'
                for col in get_schema(t.tbl):
                    if col.name not in used:
                        sel += f'{j}{t.name}.{col.name},'
                        used.add(col.name)
            query = f'{sel}\n{join}\nORDER BY{j}unitid ASC'
            # print(query)
            load_table(self.data.tbl, query=query)
            msg[1] += f'{concat_str}success'
        self.data.write()
        rpt(msg[1])
        return msg


class Ipeds():
    def __init__(self, refresh=1, years=tuple(range(19, 3, -1)), path=ROOT_PATH, **kwargs):
        self.refresh      = int(refresh)
        self.name         = 'Ipeds'
        self.path         = pathlib.Path(path)
        self.start_year   = get_full_year(min(years))
        kwargs['path']    = self.path
        kwargs['refresh'] = self.refresh
        
        self.years        = {get_full_year(year): IpedsYear(year=year, **kwargs) for year in years}
        
        kwargs['name']    = f'0vars'
        kwargs['file']    = self.path / f'{kwargs["name"]}.parquet'
        kwargs['refresh'] = self.refresh >= 3
        self.vars         = Base(**kwargs)

        kwargs['name']    = f'0rename'
        kwargs['file']    = self.path / f'{kwargs["name"]}.parquet'
        kwargs['refresh'] = self.vars.refresh
        self.rename       = Base(**kwargs)

        kwargs['name']    = f'0{self.start_year}data'
        kwargs['file']    = self.path / f'{kwargs["name"]}.parquet'
        kwargs['refresh'] = self.refresh >= 1
        self.data         = Base(**kwargs)
        rpt()

        kwargs['name']    = f'0{self.start_year}vars'
        kwargs['file']    = self.path / f'{kwargs["name"]}.parquet'
        kwargs['refresh'] = self.data.refresh
        self.vars.common  = Base(**kwargs)


    def get_data(self):
        msg  = self.data.get()
        msg2 = self.vars.common.get()
        if (msg[0] is False) or (msg2[0] is False):
            msg[1] += f'{concat_str}must get {self.vars.name}'
            rpt(msg[1])
            self.get_vars()
            
            msg[1] += f'{concat_str}must get {self.rename.name}'
            rpt(msg[1])
            self.get_rename()
            
            msg[1] += f'{concat_str}must get data for each year'
            rpt(msg[1])
            for yr, x in self.years.items():
                x.get_data()

            msg[1] += f'{concat_str}now we stack the all years data for the final combined table'
            rpt(msg[1])
            L = []
            D = []
            for yr, x in self.years.items():
                df = x.data.df.rename(columns=self.rename.dict)
                m = df.columns.duplicated(keep='first')
                df = df.loc[:, ~m]
                D.append(df)
                L.append(set(df.columns))
            common = L[0].intersection(*L)
            self.data.df = pd.concat([df[common] for df in D])
            self.vars.common.df = self.vars.df.query(f'varname in @common')
            msg[1] += f'{concat_str}success'
        self.data.write()
        self.vars.common.write()
        
        rpt(msg[1])
        return msg
    
    
    def get_vars(self):
        msg = self.vars.get()
        if msg[0] is False:
            msg[1] += f'{concat_str}must get vars for each year'
            rpt(msg[1])
            
            cols = ['varname', 'varnumber', 'year', 'vartitle', 'longdescription']
            rows = []
            for yr, x in self.years.items():
                x.get_vars()
                rows.extend(x.vars.df[cols].to_dict('records'))

            msg[1] += f'{concat_str}now we combine vars and get canonical names'
            # rpt(msg[1])
            L = []
            for row in rows:
                found = False
                for d in L:
                    if row['varnumber'] in d['alias'] or row['varname'] in d['alias']:
                        d['alias'].add(row['varnumber'])
                        d['alias'].add(row['varname'])
                        d['years'].add(row['year'])
                        if (d['year'] < row['year']) or (d['year'] == row['year'] and d['varnumber'] > row['varnumber']):
                            for c in cols:
                                d[c] = row[c]
                        found = True
                        break
                if not found:
                    row['alias'] = {row['varnumber'], row['varname']}
                    row['years'] = {row['year']}
                    L.append(row)
                          
            def f(d):
                d['count'] = len(d['years'])
                d['years'] = '|'.join(sorted(str(x) for x in d['years']))
                d['alias'] = '|'.join(sorted(str(x) for x in d['alias']))
                return d
            self.vars.df = pd.DataFrame(map(f, L)).sort_values('varnumber')
            del L
            for col in ['count', 'years', 'alias']:
                self.vars.df.insert(2, col, self.vars.df.pop(col))
            msg[1] += f'{concat_str}success'
        self.vars.write()
        rpt(msg[1])
        return msg
    

    def get_rename(self):
        try:
            self.rename.dict
            return [True, '']
        except AttributeError:
            pass
        
        msg = self.rename.get()
        if msg[0] is False:
            msg[1] += f'{concat_str}must get {self.vars.name}'
            rpt(msg[1])
            self.get_vars()

            msg[1] += f'{concat_str} getting {self.rename.name}'
            # rpt(msg[1])
            self.rename.dict = {a: d['varname'] for d in self.vars.df.to_dict('records') for a in d['alias'].split('|')}
            self.rename.df = pd.Series(self.rename.dict).reset_index()
            self.rename.df.columns = ['orig', 'canonical']
            msg[1] += f'{concat_str}success'

        self.rename.dict = self.rename.df.set_index('orig').squeeze().to_dict()
        self.rename.dict['unitid'] = 'unitid'
        self.rename.dict['year'] = 'year'
        self.rename.write()
        rpt(msg[1])
        return msg