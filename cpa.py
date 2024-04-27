import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import pandas_gbq
from google.oauth2 import service_account
 
with open("config.json") as f:
    config = json.load(f)

credentials_bq = service_account.Credentials.from_service_account_info(config["credentials"])
project_id = config['project_id']

def calculate_cpa_estimated_profit():
    estimatedProfit_cpa_query = """
    with all_ads as (
  select 
    * 
  from 
    resumedone-sys.rd_prod_staging.fct_google_ads_by_country_old
  where 
    date < '2024-03-24'
  
  union all 
  
  select 
    * 
  from 
    resumedone-sys.rd_prod_staging.fct_googleads_realtime
  where 
    date >= '2024-03-24'
),

ads_grouped as (
  select 
    date,
    campaign_id,
    campaign,
    country,
    sum(cost) as cost_google,
    sum(conversions) as trial_google,
    case
      when sum(conversions) != 0 then sum(cost) / sum(conversions)
      else 0
    end as new_cpa
  from 
    all_ads
  group by 
    date, campaign_id, campaign, country
),

life_time_value as (
  select
    country,
    sum(revenue_after_fees_and_vat) / count(distinct user_email) as LTV
  from 
    resumedone-sys.rd_prod_staging.fct_ltv_with_vat
  where
    date between '2020-01-01' and date_sub(current_date(), interval 100 day)
  group by 
    country
)

select 
  ad.date, 
  ad.campaign_id, 
  ad.campaign,
  ad.country,
  ad.new_cpa, 
  ((ltv.LTV * ad.trial_google) - ad.cost_google) as estimated_profit
from 
  ads_grouped ad
join 
  life_time_value ltv on ad.country = ltv.country;
    """
    df=pandas_gbq.read_gbq(estimatedProfit_cpa_query, project_id=project_id, credentials=credentials_bq)
    return df

def bq_upload(df):  
    dataset=config["dataset_name"]
    table_name = config["table_name"]
    table_id=project_id+'.'+dataset+'.'+table_name 
    pandas_gbq.to_gbq(
        df, table_id, project_id=project_id, credentials=credentials_bq, if_exists="replace"
    )
    print("Successfully uploaded to : ",table_id) 
df=calculate_cpa_estimated_profit()
bq_upload(df)