import pandas as pd
import pytd 
import pytd.pandas_td as pytd






TD_API_KEY = "429/8c24d345621296396b7dde58599538a0ddf534cf"
TD_API_SERVER = "https://api.treasuredata.co.jp"
con = pytd.connect(apikey=TD_API_KEY, endpoint='https://api.treasuredata.co.jp')
client = pytd.Client(TD_API_KEY,TD_API_SERVER,default_engine='presto')


print('query_block_start')
query_raw = """
select
distinct
cdp_customer_id
from
sfmc_data.sfmc_ha_series_85_inch_tv_customers_data
 """
data1 = client.query(query_raw)
df1 = pd.DataFrame(data1['data'],columns=data1['columns'])
print(df1.head(10))
print("done")

query_raw2 = """
select 
distinct
code
from
sony_src_dev.a_series_coupons

""" 
data2 = client.query(query_raw2)
df2 = pd.DataFrame(data2['data'],columns=data2['columns'])
print(df2.head(10))
print("done")

# data1 = {'customer_id' : [1,2,3]}
# data2 = {'coupon' : ['A','B','C']}



# df1 = pd.DataFrame(data1)
# df2 = pd.DataFrame(data2)

# df1['coupon'] = df2['coupon'].unique()[0]  -- Will put customer id with A
coupons = list(df2['code'])
# df1['attr1'] = coupons

# ## Using zip function which provides the result (1,A)
df1['attr1'] = list(zip(df1['cdp_customer_id'], coupons))

print('done')

# client.load_table_from_dataframe(df1,'sony_src_dev.sfmc_ha_series_85_inch_coupon_data',writer='bulk_import',if_exists='append')

# ## Modify the column name for a cleaner output
# # df1 = df1.rename(columns= {'coupon':'customer_coupon'})


# ## Separate the zip function
df1['attr3'] = df1.apply(lambda row: 
	f"{row['cdp_customer_id']} {row['attr1'][1]}",axis = 1)
# df1 = df1.drop(columns = ['customer_coupon'])

print(df1)

client.load_table_from_dataframe(df1,'sony_src_dev.sfmc_ha_series_85_customers_coupons',writer='bulk_import',if_exists='append')