# %%
from woocommerce import API
import pandas as pd
import environ
from datetime import datetime, timedelta
from .countries import add_country_and_phone
from .bq import append_df_to_bq
import json
from datetime import date

# %%
environ.Env.read_env()

# %%
env = environ.Env()

# %%
WOO_CONSUMER_KEY = env('WOO_CONSUMER_KEY')
WOO_CONSUMER_SECRET = env('WOO_CONSUMER_SECRET')
WOO_WEBSITE_URL = env('WEBSITE_URL')
# %%
def woo_init():
    wcapi = API(
        url=WOO_WEBSITE_URL,
        consumer_key=WOO_CONSUMER_KEY,
        consumer_secret=WOO_CONSUMER_SECRET,
        wp_api=True,
        version="wc/v3",
        timeout=30
    )
    return wcapi

wcapi = woo_init()

# %%
def fetch_orders_for_date(date_str):
    all_orders = []
    page = 1
    while True:
        params = {
            "per_page": 100,
            "page": page,
            "after": f"{date_str}T00:00:00Z",
            "before": f"{date_str}T23:59:59Z"
        }
        orders = wcapi.get("orders", params=params).json()
        if not orders:
            break  # Exit loop if no more orders
        all_orders.extend(orders)
        page += 1
    # Convert list of orders to DataFrame
    df_orders = pd.DataFrame(all_orders)
    return df_orders

# %%
def get_promocode(row):
    for item in row['meta_data']:
        if item['key'] in ['AffiliateCouponCode', 'CouponCode'] and item['value']:
            return item['value']
    return ''

# %%
def convert_values_to_string(meta_data):
    for item in meta_data:
        if isinstance(item['value'], (dict, list)):
            item['value'] = json.dumps(item['value'])
    return meta_data


# %%
def modify_df(df):
    df = pd.concat([df.drop(['billing'], axis=1), df['billing'].apply(pd.Series)], axis=1)
    df['meta_data'] = df['meta_data'].apply(convert_values_to_string)
    df['promocode'] = df.apply(get_promocode, axis=1)
    return df


# %%
def get_woo_orders(date, column_types):
    df = fetch_orders_for_date(date)
    if not len(df) == 0:
        df = modify_df(df)
        df = add_country_and_phone(df)
        df = df.astype(column_types)
        return df
    else: 
        print('No orders')
        return None


# %%
def woo_fetch_and_append(date, 
                         column_types, 
                         bq_schema, 
                         dataset, 
                         table, 
                         partition_field
                         ):
    
    df = fetch_orders_for_date(date)
    if not len(df) == 0:
        df = modify_df(df)
        df = add_country_and_phone(df)
        df = df.astype(column_types)
        append_df_to_bq(dataset, table, bq_schema, df, partition_field)
    else:
        print('Empty df')


# %%
def update_orders_yesterday(column_types, 
                            bq_schema, 
                            dataset, 
                            table, 
                            partition_field):
    today = date.today()
    yesterday =  today - timedelta(days=1)
    yesterday = yesterday.strftime('%Y-%m-%d')
    
    woo_fetch_and_append(yesterday,
                         column_types, 
                         bq_schema, 
                         dataset=dataset, 
                         table=table, 
                         partition_field=partition_field
                         )
