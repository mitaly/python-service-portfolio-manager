from flask import Flask
import pandas as pd
import matplotlib.pyplot as plt
from prophet import Prophet
import numpy as np
import asyncio
import aio_pika
import json
import os
from datetime import datetime
import pika
from functools import partial
import warnings
from sqlalchemy.exc import SAWarning

# Suppress the specific warning
warnings.filterwarnings(
    "ignore",
    category=SAWarning,
    message=r"relationship 'Budget\.recurrence' will copy column budgets\.guid to column recurrences\.obj_guid.*"
)

# Only import piecash *after* suppressing warnings
import piecash
async def main():
    rabbitmq_host = '192.168.1.8' 
    rabbitmq_username = 'shubham'
    rabbitmq_password = 'shubham'
    rabbitmq_port = 5672

    async_connection = await aio_pika.connect_robust(
        f"amqp://shubham:shubham@{rabbitmq_host}:5672/"
    )
  
    credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)

    sync_connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=rabbitmq_host,
        port=rabbitmq_port,
        credentials=credentials,
        virtual_host='/' # Default virtual host, change if using a different one
    ))
    output_channel = sync_connection.channel()
   
    async with async_connection:
        channel = await async_connection.channel()
        # Limit prefetch count for concurrency control
        await channel.set_qos()
        queue = await channel.declare_queue(
            "portfolio-request",
            durable=True
        )

        # start consuming
        #You must explicitly acknowledge each message after it's processed — or it gets requeued or lost on crash.
        wrapped_handler = partial(handle_message, output_channel=output_channel)
        await queue.consume(wrapped_handler, no_ack=False)
        print("[*] Waiting for messages. To exit press CTRL+C")

        # keep the program alive
        await asyncio.Event().wait()
    # analyse_trend({
    #     "accountName":"Expenses:Food Essentials",
    #     "startDate":"21-01-2024",
    #     "endDate":"31-01-2025",
    #     "job_id":"12345"
    # }, output_channel);

async def handle_message(message: aio_pika.IncomingMessage, output_channel):
    async with message.process():  # acknowledges or NACKs automatically
        try:
            data = json.loads(message.body)
            # Your actual processing logic here
            print(f"[x] Received record: {data}")
            analyse_trend(data, output_channel)
            # await asyncio.sleep(1)  # simulate I/O work
        except Exception as e:
            print(f"[!] Failed to process message: {e}")
            message.nack(requeue=False)

def get_descendents(account):
  all_accounts = [account];
  for subaccount in account.children:
    all_accounts.extend(get_descendents(subaccount));
  return all_accounts;

def analyse_trend(data,output_channel):
    # piecash can only read a gnucash file stored in sqlite format not xml or compressed.
    with piecash.open_book(data['path'], open_if_lock=True) as mybook:        # Iterate over all accounts
        account_name=data['accountName']
        start_date_str=data['startDate']
        end_date_str=data['endDate']

        account = next((acc for acc in mybook.accounts if acc.fullname == account_name), None)
        if not account:
            print(f"Account {account_name} not found.");
            exit();
        all_accounts = get_descendents(account);
        all_splits = []
        for acc in all_accounts:
            all_splits.extend(acc.splits);
  
        start_date = datetime.strptime(start_date_str, "%d-%m-%Y").date();
        end_date = datetime.strptime(end_date_str, "%d-%m-%Y").date();

        rows =[]
        for split in all_splits:
            txn = split.transaction
            txn_date = txn.post_date

            if start_date <= txn_date <= end_date:
                rows.append({
                    "ds": txn_date,
                    "description": txn.description,
                    "y": float(split.value),
                    "currency": split.account.commodity.mnemonic,
                });
        # Convert to DataFrame
        raw_df = pd.DataFrame(rows)

        # Sort by date
        raw_df.sort_values("ds", inplace=True)
        print(raw_df.head(10).to_string())
        #TODO: add missing months, dates in between according to calendar 
        job_id = data["job_id"]

        raw_df['ds'] = pd.to_datetime(raw_df['ds'], format='%d-%m-%Y')
        df_summed = raw_df.groupby(['ds'])['y'].sum().reset_index()

        model = Prophet()
        model.fit(df_summed)
        forecast_df = model.predict(df_summed)
        now = datetime.now()
        forecast_output_path = f"{job_id}_forecast_output_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        forecast_df.to_csv(forecast_output_path, index=False)
        print("Forecast file saved to ", forecast_output_path)

        df_summed['month'] = df_summed['ds'].dt.to_period('M');
        monthly_totals = df_summed.groupby('month')['y'].sum()
        average_monthly_expense = monthly_totals.mean()
        monthly_totals.to_csv(f"{job_id}_monthly_totals_{now.strftime('%Y%m%d_%H%M%S')}.csv", index=True);

        df_summed['week'] = df_summed['ds'].dt.to_period('W');
        weekly_totals = df_summed.groupby('week')['y'].sum()
        average_weekly_expense = weekly_totals.mean()
        weekly_totals.to_csv(f"{job_id}_weekly_totals_{now.strftime('%Y%m%d_%H%M%S')}.csv", index=True);

        forecast_df['month'] = forecast_df['ds'].dt.to_period('M')
        monthly_trend = forecast_df.groupby('month')['trend'].mean().reset_index()
        # Before any rolling or slicing:
        monthly_trend = monthly_trend.sort_values('month').reset_index(drop=True)
        monthly_trend['month'] = monthly_trend['month'].dt.to_timestamp()

        total_months = monthly_trend['month'].nunique()
        max_recent = min(9, total_months//3);

        unique_months_window = set()
        unique_months_window.update(range(1, total_months+1));
       
        #rolling averages
        for n in unique_months_window:   
            monthly_trend[f'rolling_trend_{n}_month'] = monthly_trend['trend'].rolling(window=n).mean()

        monthly_trend_output_path = f"{job_id}_monthly_trend_output_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        monthly_trend.to_csv(monthly_trend_output_path, index=False)
        print("Monthly trend saved to ", monthly_trend_output_path)

        insights = [];
        for recent_n in range(1, max_recent + 1):  # recent should be small
            min_prior = recent_n + 1  # must be longer than recent
            max_prior = total_months - recent_n  # leave room for recent
           
            for prior_n in range(min_prior, max_prior + 1):
                recent_col = f'rolling_trend_{recent_n}_month';
                prior_col = f'rolling_trend_{prior_n}_month';

                if recent_col not in monthly_trend.columns or prior_col not in monthly_trend.columns:
                    continue;
                # not having enough data to compare
                if len(monthly_trend) < prior_n + recent_n:
                    continue;
                recent_avg_value = monthly_trend[recent_col].iloc[-1];
                prior_index = -(recent_n+1);
                prior_avg_value = monthly_trend[prior_col].iloc[prior_index];

                if pd.isna(recent_avg_value) or pd.isna(prior_avg_value) or prior_avg_value == 0:
                    continue;
                pct_change = ((recent_avg_value - prior_avg_value) / prior_avg_value) * 100;
                insights.append({
                    'recent_n': recent_n,
                    'prior_n': prior_n,
                    'recent_avg_value': recent_avg_value,
                    'prior_avg_value': prior_avg_value,
                    'pct_change': pct_change
                });
        
        top_positive_insights = sorted([i for i in insights if i['pct_change'] > 0 ], 
        key=lambda x: x['pct_change'], reverse=True)[:1];

        top_negative_insights = sorted([i for i in insights if i['pct_change'] < 0 ],
        key=lambda x: x['pct_change'])[:1];

        print("Top Positive Insights: ", top_positive_insights);
        print("Top Negative Insights: ", top_negative_insights);

        response_body = {
            "forecast_file_path": forecast_output_path,
            "monthly_trend_file_path": monthly_trend_output_path,
            "job_id": job_id,
            "top_positive_insights": top_positive_insights,
            "top_negative_insights": top_negative_insights,
            "average_monthly_expense": average_monthly_expense,
            "average_weekly_expense": average_weekly_expense
        };
        output_channel.basic_publish(exchange='', routing_key='portfolio-response',
        body=json.dumps(response_body));
        print(f"Response sent for job_id: {job_id}, response: {response_body}");


    # categories = raw_df[category_col_name].unique()
    # df_summed = raw_df.groupby(['ds', category_col_name])['y'].sum().reset_index()

    # for cat in categories:
    #     df_cat = df_summed[df_summed[category_col_name] == cat][['ds', 'y']]
    #     print(f"<<Category{cat}>>")
    #     if df_cat.shape[0] < 2 or df_cat['ds'].nunique() == 1:
    #         continue;
    #     model = Prophet()
    #     model.add_country_holidays(country_name='IN')
    #     model.fit(df_cat)

    #     forecast = model.predict(df_cat)

    #     forecasts[cat] = {
    #         'model': model,
    #         'forecast': forecast
    #     }
    #     forecast_path = job_id+"/forecast_output_"+cat+".csv"
    #     forecast.to_csv(forecast_path, index=False)

    #     # Track trend change
    #     trend_start = forecast['trend'].iloc[0]
    #     trend_end = forecast['trend'].iloc[-1]
    #     percent_change = ((trend_end - trend_start) / trend_start) * 100
    #     trend_changes_category[cat] = percent_change

    #     # get category wise summmary- weekly and yearly trend summary
    #     summary_category[cat] = summarize_catgeory_trends(cat, forecast)

    # # ---  Compare Trend Changes Between Categories ---
    # sorted_trends = sorted(trend_changes_category.items(), key=lambda x: x[1], reverse=True);
    # # Highlight top and bottom movers
    # top = sorted_trends[0]
    # bottom = sorted_trends[-1]
    # high_level_summary = {
    #     'highest_growth': {
    #         'category': top[0],
    #         'value': top[1]
    #     },
    #     'highest_drop': {
    #         'category': bottom[0],
    #         'value': bottom[1]
    #     },
    # }

    # return {
    #     'forecasts': forecasts,
    #     'trend_changes_category': trend_changes_category,
    #     'summary_category': summary_category,
    #     'high_level_summary': high_level_summary
    # }

def summarize_catgeory_trends(category, forecast_df):
    response = {}
    trend = forecast_df[['ds', 'trend']].dropna()
    start_trend = trend['trend'].iloc[0]
    end_trend = trend['trend'].iloc[-1]
    delta = end_trend - start_trend
    percent = ((end_trend - start_trend) / start_trend) * 100

    if percent > 25:
        response['increase_trend'] = percent;
    elif percent < -25:
        response['decrease_trend'] = percent;
    else:
        response['stable_trend'] = percent;
    if 'weekly' in forecast_df.columns:
        weekly = forecast_df.groupby(forecast_df['ds'].dt.dayofweek)['weekly'].mean()
        max_day = weekly.idxmax()
        min_day = weekly.idxmin()
        delta_val = weekly[max_day] - weekly[min_day]
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        response['highest_spend_weekly'] = {
            'day': day_names[max_day],
            'value': weekly[max_day]
        }
        response['lowest_spend_weekly'] = {
            'day': day_names[min_day],
            'value': weekly[min_day]
        }

    if 'yearly' in forecast_df.columns:
        monthly = forecast_df.groupby(forecast_df['ds'].dt.month)['yearly'].mean()
        peak_month = monthly.idxmax()
        low_month = monthly.idxmin()
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        response['highest_spend_weekly'] = {
            'day': months[peak_month-1],
            'value': monthly[peak_month]
        }
        response['lowest_spend_weekly'] = {
            'day': months[low_month-1],
            'value': monthly[low_month]
        }
     
    return response;

if __name__ == "__main__":
    asyncio.run(main())
    