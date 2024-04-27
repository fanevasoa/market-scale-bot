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


def get_all_campaign():
    campaigns = """
      with all_ads as ( 
        select *   from 
          resumedone-sys.rd_prod_staging.fct_google_ads_by_country_old 
          where     date < '2024-03-24'   
        union all   
        select     *   from     
          resumedone-sys.rd_prod_staging.fct_googleads_realtime  
          where     date >= '2024-03-24') 
        SELECT DISTINCT campaign, campaign_id, country, date 
        from all_ads ORDER BY date DESC LIMIT 20;
      """
    df = pandas_gbq.read_gbq(campaigns, project_id=project_id, credentials=credentials_bq)
    return df


def get_real_cpa(campaign_id):
    real_cpa_query_response = """
    with all_ads as (  
          select *   
          from resumedone-sys.rd_prod_staging.fct_google_ads_by_country_old  
          where date < '2024-03-24'    
          union all     
          select *   
          from resumedone-sys.rd_prod_staging.fct_googleads_realtime  
          where   date >= '2024-03-24'
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
        from all_ads  
        group by date, campaign_id, campaign, country
      ),
      life_time_value as (  
        select    
          country,    
          sum(revenue_after_fees_and_vat) / count(distinct user_email) as LTV  
        from resumedone-sys.rd_prod_staging.fct_ltv_with_vat  
        where   
          date between '2020-01-01' and date_sub(current_date(), interval 100 day)  
          group by country
        ) 
        select   AVG(ad.new_cpa) as real_cpa from   ads_grouped ad 
        join   life_time_value ltv on ad.country = ltv.country 
        where ad.campaign_id =  {campaign_id};
      """
    df = pandas_gbq.read_gbq(real_cpa_query_response, project_id=project_id, credentials=credentials_bq)
    return 0


def get_budget(campaign_id):
    # TODO: connect to google ads campaign managment and retrieve budget
    # make post request on
    url = '/v16/customers/1755456144/googleAds:searchStream'
    body = """
  {
    "query" : " 
      SELECT 
        campaign.id, 
        campaign.name, 
        campaign_budget.amount_micros, 
        campaign_budget.status, 
        campaign_budget.type, 
        campaign_budget.id, 
        campaign_budget.recommended_budget_amount_micros, 
        campaign_budget.period 
      FROM campaign 
      WHERE campaign.id = {campaign_id}
    "
  }
  """
    return 0


def store_cpa_budget_initial_value(budget, optimal_cpa, campaign_id, campaign_name):
    # TODO: storing data  in bigquery
    # budget, campaign_id, optimal_cpa,
    return


def get_previous_cpa_budget(campaign_id, campaign_name):
    cpa_budget = """
      SELECT    
        budget,    
        optimal_cpa,    
        CONCAT(        
          EXTRACT(YEAR FROM created_at), '-', 
          LPAD(CAST(EXTRACT(MONTH FROM created_at) AS STRING), 2, '0'), '-',
          LPAD(CAST(EXTRACT(DAY FROM created_at) AS STRING), 2, '0'), ' ',
          LPAD(CAST(EXTRACT(HOUR FROM created_at) AS STRING), 2, '0'), ':',
          LPAD(CAST(EXTRACT(MINUTE FROM created_at) AS STRING), 2, '0'), ':',
          LPAD(CAST(EXTRACT(SECOND FROM created_at) AS STRING), 2, '0') 
        ) AS cat 
        FROM `resumedone-sys.market_bot_data.decision_data` 
        WHERE campaign_id = {campaign_id} and budget is not null and optimal_cpa is not null 
        ORDER BY created_at DESC;
  """
    df = pandas_gbq.read_gbq(cpa_budget, project_id=project_id, credentials=credentials_bq)
    # TODO: get previous_optimal_cpa and previous_budget from the query result
    previous_optimal_cpa = 0
    previous_budget = 0
    budget = get_budget(campaign_id)
    optimal_cpa = get_real_cpa(campaign_id)

    # if the query doesn't return anything
    previous_budget = budget * 0.98
    previous_optimal_cpa = optimal_cpa * 0.98
    store_cpa_budget_initial_value(previous_budget, previous_optimal_cpa, campaign_id, campaign_name)

    return {
        'budget': budget,
        'optimal_cpa': optimal_cpa,
        'previous_optmal_cpa': previous_optimal_cpa,
        'previous_budget': previous_budget,
        'date_move': previous_budget,
    }


def get_estimated_profit(campaign_id, start_date, end_date):
    query = """
     with all_ads as (  
        select *   
        from resumedone-sys.rd_prod_staging.fct_google_ads_by_country_old  
        where     
            date < '2024-03-24'    
        union all     
        select     *   
        from     resumedone-sys.rd_prod_staging.fct_googleads_realtime  
        where     date >= '2024-03-24'
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
                 when 
                    sum(conversions) != 0 
                 then 
                    sum(cost) / sum(conversions)      
                 else 0    
             end as new_cpa  
         from  all_ads  
         group by     
            date, 
            campaign_id, 
            campaign, 
            country
        ),
    life_time_value as (  
        select    
            country,    
            sum(revenue_after_fees_and_vat) / count(distinct user_email) as LTV  
        from     resumedone-sys.rd_prod_staging.fct_ltv_with_vat  
        where    date between '2020-01-01' and date_sub(current_date(), interval 100 day)  
        group by     country
        )
    select   
        ad.date,   
        ad.campaign_id,   
        ad.campaign,  
        ad.country,  
        ad.new_cpa,   
        ((ltv.LTV * ad.trial_google) - ad.cost_google) as estimated_profit 
    from   ads_grouped ad 
    join   life_time_value ltv on ad.country = ltv.country      
    where ad.campaign_id = {campaign_id}     
    order by ad.date desc LIMIT 7;
    """


SCHEDULE_EXECUTION = 1  # lunch every how many days


def get_estimated_profit_before_movement(campaign_id):
    get_estimated_profit(campaign_id, start_date='', end_date='')
    return 0


def get_estimated_profit_after_movement(campaign_id):
    get_estimated_profit(campaign_id, start_date='', end_date='')
    return 0


PROFIT_MOVEMENT = ['stable', 'increase', 'decrease', 'unstable']
COEFFICIENT = 5  # in %


def detect_profit_movement(data):
    if len(data) < 2:
        print("Insufficient data points")
        return None

    threshold = data[0] * COEFFICIENT / 100
    trend = None

    for i in range(1, len(data)):
        diff = data[i] - data[i - 1]

        if abs(diff) <= threshold:
            continue

        if diff > 0:
            if trend is None or trend == PROFIT_MOVEMENT[1]:
                trend = PROFIT_MOVEMENT[1]
            else:
                return PROFIT_MOVEMENT[3]
        else:
            if trend is None or trend == PROFIT_MOVEMENT[2]:
                trend = PROFIT_MOVEMENT[2]
            else:
                return PROFIT_MOVEMENT[3]

    if trend is None:
        trend = PROFIT_MOVEMENT[0]

    return trend


INCREASE_COEFFICIENT = 1.02
DECREASE_COEFFICIENT = 0.98


def decide(
        previous_optimal_cpa,
        optimal_cpa,
        previous_budget,
        budget,
        real_cpa,
        profit_movement,
):
    increased_optimal_cpa = optimal_cpa * INCREASE_COEFFICIENT
    decreased_optimal_cpa = optimal_cpa * DECREASE_COEFFICIENT
    increased_budget = budget * INCREASE_COEFFICIENT
    decreased_budget = budget * DECREASE_COEFFICIENT

    decision = {}
    # if previous move is to incerase optimal CPA OR if (optimal CPA is not changed & budget increase)
    if previous_optimal_cpa < optimal_cpa or (previous_optimal_cpa != optimal_cpa and previous_budget < budget):
        # If profit increases
        if profit_movement == PROFIT_MOVEMENT[1]:
            if optimal_cpa == real_cpa:
                decision['action_optimal_cpa'] = 'increase optimal CPA'
                decision['new_optimal_cpa'] = increased_optimal_cpa
                decision['action_budget'] = 'increase budget'
                decision['new_budget'] = increased_budget
            elif real_cpa < optimal_cpa:
                decision['action_optimal_cpa'] = 'optimal CPA not changed'
                decision['new_optimal_cpa'] = optimal_cpa
                decision['action_budget'] = 'increase budget'
                decision['new_budget'] = increased_budget
            elif optimal_cpa < real_cpa:
                decision['action_optimal_cpa'] = 'increase optimal CPA'
                decision['new_optimal_cpa'] = increased_optimal_cpa
                decision['action_budget'] = 'budget not changed'
                decision['new_budget'] = budget

        # profit is stable
        elif profit_movement == PROFIT_MOVEMENT[0]:
            if optimal_cpa == real_cpa:
                decision['action_optimal_cpa'] = 'decrease optimal CPA'
                decision['new_optimal_cpa'] = decreased_optimal_cpa
                decision['action_budget'] = 'decrease budget'
                decision['new_budget'] = decreased_budget
            elif optimal_cpa > real_cpa:
                decision['action_optimal_cpa'] = 'optimal CPA not changed'
                decision['new_optimal_cpa'] = optimal_cpa
                decision['action_budget'] = 'increase budget'
                decision['new_budget'] = increased_budget
            elif optimal_cpa < real_cpa:
                decision['action_optimal_cpa'] = 'optimal CPA not change'
                decision['new_optimal_cpa'] = optimal_cpa
                decision['action_budget'] = 'decrease budget'
                decision['new_budget'] = decreased_budget

        #  profit decreased
        elif profit_movement == PROFIT_MOVEMENT[2]:
            decision['action_optimal_cpa'] = 'decrease optimal CPA'
            decision['new_optimal_cpa'] = decreased_optimal_cpa
            decision['action_budget'] = 'decrease budget'
            decision['new_budget'] = decreased_budget

    # if previous move is to reduce optimal CPA OR if(optimal CPA is not changed & budget decrease)
    elif previous_optimal_cpa > optimal_cpa or (previous_optimal_cpa == optimal_cpa and previous_budget < budget):
        #  profit increases
        if profit_movement == PROFIT_MOVEMENT[1]:
            if real_cpa <= optimal_cpa:
                decision['action_optimal_cpa'] = 'decrease optimal CPA'
                decision['new_optimal_cpa'] = decreased_optimal_cpa
                decision['action_budget'] = 'decrease budget'
                decision['new_budget'] = decreased_budget
            elif real_cpa > optimal_cpa:
                decision['action_optimal_cpa'] = 'optimal CPA not change'
                decision['new_optimal_cpa'] = optimal_cpa
                decision['action_budget'] = 'decrease budget'
                decision['new_budget'] = decreased_budget

        elif profit_movement == PROFIT_MOVEMENT[0]:
            if optimal_cpa == real_cpa:
                decision['action_optimal_cpa'] = 'increase optimal CPA'
                decision['new_optimal_cpa'] = increased_optimal_cpa
                decision['action_budget'] = 'increase budget'
                decision['new_budget'] = increased_budget

            elif real_cpa > optimal_cpa:
                decision['action_optimal_cpa'] = 'optimal CPA not change'
                decision['new_optimal_cpa'] = optimal_cpa
                decision['action_budget'] = 'decrease budget'
                decision['new_budget'] = decreased_budget

            elif real_cpa < optimal_cpa:
                decision['action_optimal_cpa'] = ' optimal CPA not change'
                decision['new_optimal_cpa'] = optimal_cpa
                decision['action_budget'] = 'increase budget'
                decision['new_budget'] = increased_budget

        elif profit_movement == PROFIT_MOVEMENT[2]:
            decision['action_optimal_cpa'] = 'increase optimal CPA'
            decision['new_optimal_cpa'] = increased_optimal_cpa
            decision['action_budget'] = 'increase budget'
            decision['new_budget'] = increased_budget

    return decision


def create_record_in_logs(decision, campaign_id, campaign_name):
    create_record_in_big_query(data, table)


def run():
    campaign_data = get_all_campaign()
    for campaign in campaign_data:
        campaign_id = campaign['campaign_id']
        campaign_name = campaign['campaign_name']
        real_cpa = get_real_cpa(campaign_id)
        budget = get_budget(campaign_id)
        previous_optimal_cpa_budget = get_previous_cpa_budget(campaign_id, campaign_name)
        profit_movement=detect_profit_movement(data_profit)
        decision = decide(
            previous_optimal_cpa_budget.get('previous_optimal_cpa'),
            previous_optimal_cpa_budget.get('optimal_cpa'),
            previous_optimal_cpa_budget.get('previous_budget'),
            previous_optimal_cpa_budget.get('budget'),
            real_cpa,
            profit_movement
        )
        create_record_in_logs(decision, campaign_id, campaign_name)



def bq_upload(df):
    """
    Upload to resumedone-sys.rd_src_indicative_user.purchasers_utm_sources
    """
    try:
        credentials = service_account.Credentials.from_service_account_info(
            config["credentials"]
        )
        project_id = config["project_id"]
        table_id = config["logs"]
        df.loc[df['Country'] == 'Vietnam', 'Country'] = 'Viet Nam'
        pandas_gbq.to_gbq(
            df, table_id, project_id=project_id, credentials=credentials, if_exists="append"
        )
        print("Successfully uploaded to Bigquery!")
        return True
    except:
        print("Upload to Bigquery failed!")
        return False


if __name__ == '__main__':
    run()
