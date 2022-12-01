from dotenv import load_dotenv
load_dotenv()
import os
import json
import time
import logging
import logzero
import numpy as np
import pandas as pd
import pyodbc
import subprocess
import json


def flatten_json(nested_json, exclude=['']):
    out = {}

    def flatten(x, name='', exclude=exclude):
        if type(x) is dict:
            for a in x:
                if a not in exclude: flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out    

if __name__ == '__main__':
    logzero.loglevel(logging.WARN)

    try:
        from akeneo_api_client.client import Client
    except ModuleNotFoundError as e:
        import sys
        sys.path.append("..")
        from akeneo_api_client.client import Client

    AKENEO_CLIENT_ID = os.environ.get("AKENEO_CLIENT_ID")
    AKENEO_SECRET = os.environ.get("AKENEO_SECRET")
    AKENEO_USERNAME = os.environ.get("AKENEO_USERNAME")
    AKENEO_PASSWORD = os.environ.get("AKENEO_PASSWORD")
    AKENEO_BASE_URL = os.environ.get("AKENEO_BASE_URL")
    sage_conn_str = os.environ.get(r"sage_conn_str").replace("UID=;","UID=" + os.environ.get(r"sage_login") + ";").replace("PWD=;","PWD=" + os.environ.get(r"sage_pw") + ";") 

    akeneo = Client(AKENEO_BASE_URL, AKENEO_CLIENT_ID,AKENEO_SECRET, AKENEO_USERNAME, AKENEO_PASSWORD)             
    cnxn = pyodbc.connect(sage_conn_str, autocommit=True)

    sql = "SELECT TaxClass FROM SY_SalesTaxClass WHERE TaxClass <> 'NT' AND TaxClass <> 'TX' AND TaxClass <> 'TF' AND TaxClass <> 'SV'" 
    SageQueryDF = pd.read_sql(sql,cnxn)
    taxclasses = SageQueryDF['TaxClass'].tolist()
    print(SageQueryDF)
    today_tax_df = pd.DataFrame(data=None)
    for taxclass in taxclasses:
        akeneo_Tax_Group = "Tax_Exception_" + taxclass
        print(akeneo_Tax_Group)    
        searchparams = """
        {
            "limit": 100,
            "locales": "en_US",
            "with_count": true,
            "attributes": "Brand",
            "search": {
                "groups": [{
                    "operator": "IN",
                    "value": ["tax_classes_Group"]
                }]
            }
        }
        """.replace("tax_classes_Group",akeneo_Tax_Group)
        result = akeneo.products.fetch_list(json.loads(searchparams))

        go_on = True
        count = 0
        while go_on:
            count += 1
            try:
                print(str(count) + ": normalizing")
                page = result.get_page_items()
                pagedf = pd.DataFrame([flatten_json(x,['scope','locale','currency','unit']) for x in page])
                pagedf.columns = pagedf.columns.str.replace('values_','')
                pagedf.columns = pagedf.columns.str.replace('_0','')
                pagedf.columns = pagedf.columns.str.replace('_data','')
                pagedf.columns = pagedf.columns.str.replace('_amount','')
                pagedf.columns = pagedf.columns.str.replace('associations_','')
                pagedf.columns = pagedf.columns.str.replace('_products','')
                pagedf['TaxClass'] = taxclass
                pagedf.drop(pagedf.columns.difference(["identifier","TaxClass"]), 1, inplace=True)
                today_tax_df = today_tax_df.append(pagedf, sort=False)
            except:
                go_on = False
                break
            go_on = result.fetch_next_page()

    today_tax_df = today_tax_df.set_index('identifier').sort_index()
    last_tax_df = pd.read_pickle(r'\\FOT00WEB\Alt Team\Kris\GitHubRepos\tax-feed-to-sage\LastTaxSync.p').sort_index()

    unique_tax_df = pd.concat([today_tax_df,last_tax_df],sort=False).reset_index()
    unique_tax_df = unique_tax_df.drop_duplicates(subset='identifier',keep=False)
    
    if unique_tax_df.shape[0] > 0:
        unique_tax_df = unique_tax_df.set_index('identifier')
        new_items_df = unique_tax_df[unique_tax_df.isin(today_tax_df)].dropna(how='all')
        del_items_df = unique_tax_df[~unique_tax_df.isin(today_tax_df)].dropna(how='all')
        del_items_df['TaxClass'] = 'TX'
        
        mask = ~today_tax_df.index.isin(unique_tax_df.index)
        today_tax_df_less_new = today_tax_df.loc[mask].sort_index()
        mask = ~last_tax_df.index.isin(unique_tax_df.index)
        last_tax_df_less_dead = last_tax_df.loc[mask].sort_index()

        if today_tax_df_less_new.equals(last_tax_df_less_dead) == False:
            print(today_tax_df_less_new)
            print(last_tax_df_less_dead)
            where_mask = (today_tax_df_less_new != last_tax_df_less_dead)
            akeneoDF = today_tax_df_less_new.where(cond=where_mask, other=np.nan)
            akeneoDF = akeneoDF.dropna(how='all')                   
            akeneoDF = pd.concat([akeneoDF,new_items_df,del_items_df],sort=False)
        else:
            akeneoDF = pd.concat([new_items_df,del_items_df],sort=False)
    else:
        if today_tax_df.equals(last_tax_df) == False:
            print(today_tax_df)
            print(last_tax_df)            
            where_mask = (today_tax_df != last_tax_df)

            akeneoDF = today_tax_df.where(cond=where_mask, other=np.nan)    
            akeneoDF = akeneoDF.dropna(how='all')                
        else:
            akeneoDF = pd.DataFrame(data=None)

    if akeneoDF.shape[0] > 0:            

        #sage data
        akeneoDF.to_csv(r'\\FOT00WEB\Alt Team\Qarl\Automatic VI Jobs\AkeneoSync\from_akeneo_tax_sync_VIWI5Y.csv', header=False, sep=',', index=True) 
        print('to csv')
        time.sleep(15) 
        p = subprocess.Popen('Auto_SyncAkeneoTaxs_VIWI5Y.bat', cwd= r'Y:\Qarl\Automatic VI Jobs\AkeneoSync', shell = True)
        stdout, stderr = p.communicate()   
        print('to sage')
        today_tax_df.to_pickle(r'\\FOT00WEB\Alt Team\Kris\GitHubRepos\tax-feed-to-sage\LastTaxSync.p')
        print('pickled')
    else:
        print('nothing to sync')
    print('done!')