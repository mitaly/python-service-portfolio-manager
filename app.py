from flask import Flask
import pandas as pd
import matplotlib.pyplot as plt
from prophet import Prophet
import numpy as np

import asyncio
import aio_pika
import json

async def main():
    connection = await aio_pika.connect_robust(
        "amqp://shubham:shubham@192.168.1.30/"
    )

    async with connection:
        channel = await connection.channel()
        # Limit prefetch count for concurrency control
        await channel.set_qos()
        queue = await channel.declare_queue(
            "hello",
            durable=True
        )

        # start consuming
        #You must explicitly acknowledge each message after it's processed — or it gets requeued or lost on crash.
        await queue.consume(handle_message, no_ack=False)
        print("[*] Waiting for messages. To exit press CTRL+C")

        # keep the program alive
        await asyncio.Event().wait()

async def handle_message(message: aio_pika.IncomingMessage):
    async with message.process():  # acknowledges or NACKs automatically
        try:
            data = json.loads(message.body)
            # Your actual processing logic here
            print(f"[x] Received record: {data}")
            analyse_trend(data)
            # await asyncio.sleep(1)  # simulate I/O work
        except Exception as e:
            print(f"[!] Failed to process message: {e}")
            message.nack(requeue=False)

def analyse_trend(data):
    # --- 1. Load Data ---
    file_path = data["filePath"]

    # Load the CSV
    raw_df = pd.read_csv(file_path)
    raw_df['ds'] = pd.to_datetime(raw_df['Date'], format='%d-%m-%Y')
    raw_df['y'] = raw_df['Amount Num.']
    category_col_name = 'Full Category Path';
    forecasts = {}
    trend_changes_category = {}
    summary_category = {}
    high_level_summary = {}
    # this catgeory is incorrect.- pass a category field in a column.
    categories = raw_df[category_col_name].unique()
    df_summed = raw_df.groupby(['ds', category_col_name])['y'].sum().reset_index()

    for cat in categories:
        df_cat = df_summed[df_summed[category_col_name] == cat][['ds', 'y']]
        print(f"<<Category{cat}>>")
        if df_cat.shape[0] < 2 or df_cat['ds'].nunique() == 1:
            continue;
        model = Prophet()
        model.add_country_holidays(country_name='IN')
        model.fit(df_cat)

        forecast = model.predict(df_cat)

        forecasts[cat] = {
            'model': model,
            'forecast': forecast
        }

        # Track trend change
        trend_start = forecast['trend'].iloc[0]
        trend_end = forecast['trend'].iloc[-1]
        percent_change = ((trend_end - trend_start) / trend_start) * 100
        trend_changes_category[cat] = percent_change

        # get category wise summmary- weekly and yearly trend summary
        summary_category[cat] = summarize_catgeory_trends(cat, forecast)

    # ---  Compare Trend Changes Between Categories ---
    sorted_trends = sorted(trend_changes_category.items(), key=lambda x: x[1], reverse=True);
    # Highlight top and bottom movers
    top = sorted_trends[0]
    bottom = sorted_trends[-1]
    high_level_summary = {
        'highest_growth': {
            'category': top[0],
            'value': top[1]
        },
        'highest_drop': {
            'category': bottom[0],
            'value': bottom[1]
        },
    }

    return {
        'forecasts': forecasts,
        'trend_changes_category': trend_changes_category,
        'summary_category': summary_category,
        'high_level_summary': high_level_summary
    }

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
    