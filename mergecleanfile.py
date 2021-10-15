## Import necessary Modules
# %%
import pandas as pd
import glob
import numpy as np
from fuzzywuzzy import fuzz
import re
import pyodbc
import sqlalchemy
pd.options.mode.chained_assignment = None

#Setup DB Connection
server = 'localhost\sqlexpress' 
database = 'pcard' 
username = 'ludwig' 
password = 'ludwig' 

constring = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
dbEngine = sqlalchemy.create_engine(constring, fast_executemany=True, connect_args={'connect_timeout': 10}, echo=False)

try:
    with dbEngine.connect() as con:
        con.execute("SELECT 1")
    print('DB Engine and Connection is Valid')
except Exception as e:
    print(f'DB Engine is Invalid: {str(e)}')

# %% [markdown]
# ### set the filepath and the wildcard mask to read all excel files

# %%
# inpath = r"C:\Users\A186999\OneDrive - Standard Bank\pcard\infiles"
# outpath = r"C:\Users\A186999\OneDrive - Standard Bank\pcard\outfiles"
# filenames = glob.glob(f"{inpath}\*.xlsx")

#Directory setup
inpath = r".\infiles"
outpath = r".\outfiles"
filenames = glob.glob(f"{inpath}\\*.xlsx")

# %% [markdown]
# #### Create a Function to Load files which takes a list parameter (The directory where new files are dropped daily/monthly)
# 
# #### Initialize an Empty Dataframe that would have the data from all the excel files. 
# ##### A Column list is also created to make the different column names uniform during merge
# 
# #### Using a for-loop, we loop through the directory, reading an excel file, rename the column names for consistency and appending it to the CombinedFrame DF created earlier. 
# #### We also know from trend that any line without a division-name is for calculations/subtotals only. We would go ahead and remove those immediately as we load the files

# %%
def MergeFilesInitialLoad(filenames):
    combinedFrame = pd.DataFrame()
    col = ['division',
    'batch_id',
    'tran_date',
    'pstd_date',
    'merchant_name',
    'tran_amt',
    'tran_crncy',
    'orig_amt',
    'orig_crcny',
    'gl_acct',
    'gl_acct_desc',
    'cc_wbs_ord',
    'cc_wbs_ord_desc',
    'merchant_type',
    'merchant_type_desc',
    'purpose']

    print('loading files ...')

    for file in filenames:
        df =pd.read_excel(file, sheet_name=False)
        df.columns = col
        df = df[~df['batch_id'].isnull()]
    
        combinedFrame = combinedFrame.append(df, ignore_index=True)
    
    print('loading Completed Successfully !!!')
    return combinedFrame

## Function to clean up merchant-name a bit using fuzzy wuzzy
def match_names(name, list_names, min_score=0):
    max_score = -1
    max_name = ''
    for x in list_names:
        score = fuzz.ratio(name, x)
        if (score > min_score) & (score > max_score):
            max_name = x
            max_score = score
    return (max_name, max_score)


def cleanFrame(file):

    # combinedFrame = pd.read_csv(file, header=True)
    combinedFrame = MergeFilesInitialLoad(file)
    workFrame = combinedFrame
    
    print('Data Cleansing started!!!')

    workFrame[['division']] = workFrame[['division']].fillna(value='UNKNOWN')
    workFrame['tran_date'] = workFrame['tran_date'].ffill()
    workFrame = workFrame.drop_duplicates('batch_id', keep='last')
    
    print('Cleaning Merchant Details')

    workFrame['raw_merchant_name'] = workFrame['merchant_name']
    raw_merchant_list = list(workFrame.merchant_name.unique())
    cleaned_merchant_list = pd.read_csv('./cleanedMerchantList.csv')
    cleaned_merchant_list = list(cleaned_merchant_list['0'])

    ## Using regex to clean up the merchant names a bit before applying the fuzzy logic
    pattern = r'((#\d{1,}\s)|(\s# \d{1,}.+)|(\s#\d{1,}.+)|( # 1| #1)|(\s#\d))'
    raw_merchant_list =[re.sub(pattern, '', w) for w in raw_merchant_list]

    names = []
    for x in raw_merchant_list:
        match = match_names(x, cleaned_merchant_list, 0)
        if match[1] >= 60:
            name = ('(' + str(x), str(match[0]) + ')')
            names.append(name)
        else:
            name = ('(' + str(x), str(x)+ ')')
            names.append(name)
    name_dict = dict(names)

    #Using the dictionary to replace the keys with the values in the 'name' column for the second dataframe
    workFrame.merchant_name = workFrame.merchant_name.replace(name_dict)

    workFrame["merchant_type"] = workFrame["merchant_type"].astype('Int64')
    newMcc_df = workFrame[workFrame['merchant_type_desc']=="NEW MCC CODE"]
    withoutNewMcc_df =  workFrame[workFrame['merchant_type_desc']!="NEW MCC CODE"]
    distinct_mcc = withoutNewMcc_df['merchant_type_desc'].unique()
    df_distinct_mcc= pd.DataFrame(distinct_mcc)
    df_distinct_mcc.columns = ["merchant_type_desc"]
    df_mccframe = withoutNewMcc_df[['merchant_type_desc', 'merchant_type']]
    df_mccframe_dict = dict(df_mccframe.values)
    df_distinct_mcc['merchant_type'] = df_distinct_mcc.merchant_type_desc.map(df_mccframe_dict)

    withoutNewMcc_df = pd.merge(withoutNewMcc_df, df_distinct_mcc, on='merchant_type_desc')
    withoutNewMcc_df['merchant_type_x'] = withoutNewMcc_df['merchant_type_y']
    withoutNewMcc_df.rename(columns={'merchant_type_x':'merchant_type'},inplace=True)

    distinct_wmcc = newMcc_df['merchant_type'].unique()
    df_distinct_wmcc= pd.DataFrame(distinct_wmcc)
    df_distinct_wmcc.columns = ["merchant_type"]
    df_distinct_wmcc['merchant_type_desc'] = "NEW MCC CODE"

    newMcc_df = pd.merge(newMcc_df, df_distinct_wmcc, on='merchant_type')
    newMcc_df.rename(columns={'merchant_type_desc_x':'merchant_type_desc'},inplace=True)

    merchantWorkFrame_df = pd.concat([withoutNewMcc_df, newMcc_df])
    # merchantWorkFrame_df.to_csv(".\merchantWorkFrame.csv", index=False)

    ## We merge these two unique Merchant DF (it contains Unique Merchant Types and their descriptions) We will use to merge with a 
    #unique merchant name list in order to get the unique merchant details for our db
 
    uniquemerchantdf = pd.concat([df_distinct_wmcc, df_distinct_mcc])

    distinct_mname = merchantWorkFrame_df['merchant_name'].unique()
    df_distinct_mname= pd.DataFrame(distinct_mname)
    df_distinct_mname.columns = ["merchant_name"]
    df_mnameframe = merchantWorkFrame_df[['merchant_name', 'merchant_type']]
    df_mnameframe_dict = dict(df_mnameframe.values)
    df_distinct_mname['merchant_type'] = df_distinct_mname.merchant_name.map(df_mnameframe_dict)

    ## and here we merge the tables

    merchantsTableDf = pd.merge(df_distinct_mname, uniquemerchantdf, on='merchant_type')
    merchantsTableDf = merchantsTableDf.drop_duplicates(subset=['merchant_name'], keep='last')
    
    print('merchant details cleansed!!\nCommencing Cost Center Cleanup!\n')


    ## Cleaning and Prepping Cost Center/WBS Orders
    #### We Commence data cleaning on the CostCenter/WBS related fields

    ccWorkFramedf = merchantWorkFrame_df
    ccWorkFramedf.loc[ccWorkFramedf['cc_wbs_ord'] == '-', ['cc_wbs_ord']] = '000001'
    ccWorkFramedf.loc[ccWorkFramedf['cc_wbs_ord'].isnull(), ['cc_wbs_ord']] = '000001'  
    
    distinctCcWbs = ccWorkFramedf['cc_wbs_ord'].unique()
    df_distinctCcWbs = pd.DataFrame(distinctCcWbs)
    df_distinctCcWbs.columns = ["cc_wbs_ord"]
    df_CcWbs = ccWorkFramedf[['cc_wbs_ord', 'cc_wbs_ord_desc']]
    df_CcWbs_dict = dict(df_CcWbs.values)

    df_distinctCcWbs['cc_wbs_ord_desc'] = df_distinctCcWbs.cc_wbs_ord.map(df_CcWbs_dict)
    df_distinctCcWbs[['cc_wbs_ord_desc']] = df_distinctCcWbs[['cc_wbs_ord_desc']].fillna(value='NO DESC')

    #We also use the index function to create a cc_wbs_ord_id in line with our data model for the Cost Center Table. 
    # df_distinctCcWbs['cc_wbs_ord_id'] = df_distinctCcWbs.index
    ccWorkFramedf = pd.merge(ccWorkFramedf, df_distinctCcWbs, on='cc_wbs_ord')
    ccWorkFramedf['cc_wbs_ord_desc_x'] = ccWorkFramedf['cc_wbs_ord_desc_y']
    ccWorkFramedf.rename(columns={'cc_wbs_ord_desc_x':'cc_wbs_ord_desc'},inplace=True)

    print('Cost Center details cleansed!!\nGenerating Unique Division Names for our DW!\n\n')

    divDf = ccWorkFramedf
    distinct_div = divDf['division'].unique()
    df_distinct_div= pd.DataFrame(distinct_div)
    df_distinct_div.columns = ["division"]
    divWorkFramedf = pd.merge(divDf, df_distinct_div, on='division')
    divTableDf = df_distinct_div
    divTableDf.to_csv(f"{outpath}\\divTableDf.csv", index=False)
    divTableDf.to_sql(con=dbEngine, schema='dbo', name='division', if_exists="append", index=False, chunksize=1000)

    print('Division sorted !!!\nGenerating Table data\n\n')

       #we already create this dataset up 
    merchantsTableDf.to_csv(f"{outpath}\\merchantTableDf.csv", index=False)
    merchantsTableDf.to_sql(con=dbEngine, schema='dbo', name='merchants', if_exists="append", index=False, chunksize=1000)


    costCenterTableDf = df_distinctCcWbs
    costCenterTableDf.to_csv(f"{outpath}\\costCenterTableDf.csv", index=False)
    costCenterTableDf.to_sql(con=dbEngine, schema='dbo', name='costCenter', if_exists="append", index=False, chunksize=1000)


    ## Creating a our gl df. We would use this eventually to create and populate our General Ledger's table 

    distinct_glc = divWorkFramedf['gl_acct'].unique()
    df_distinct_glc= pd.DataFrame(distinct_glc)
    df_distinct_glc.columns = ["gl_acct"]
    df_glc = ccWorkFramedf[['gl_acct', 'gl_acct_desc']]
    df_glc_dict = dict(df_glc.values)
    df_distinct_glc['gl_acct_desc'] = df_distinct_glc.gl_acct.map(df_glc_dict)

    generalLedgerTableDf = df_distinct_glc
    generalLedgerTableDf.to_csv(f"{outpath}\\generalLedgerTableDf.csv", index=False)
    generalLedgerTableDf.to_sql(con=dbEngine, schema='dbo', name='generalLedger', if_exists="append", index=False, chunksize=1000)


    transactionDf = divWorkFramedf
    transactionTableDf = transactionDf[["batch_id", "division", "tran_date", "pstd_date", "merchant_name", "tran_amt", "tran_crncy", "orig_amt", "orig_crcny", "gl_acct", "cc_wbs_ord", "purpose"]]
    
    print('writing purchases details to file and table')
    transactionTableDf.to_csv(f"{outpath}\\transactionTableDf.csv", index=False)
    transactionTableDf.to_sql(con=dbEngine, schema='dbo', name='purchases', if_exists="append", index=False, chunksize=1000)


    print('All Done!!!')
    return transactionDf


divDf = cleanFrame(filenames)
print(divDf.head(5))
