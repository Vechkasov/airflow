from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


import datetime
import requests
import matplotlib.pyplot as plt
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime.datetime(2024, 9, 3),
    'schedule_interval': '@daily',
}


def fetch_weather_data(key, lat, lon):
    url = f"https://api.weather.yandex.ru/v2/forecast?lat={lat}&lon={lon}&limit=7&hours=false"
    headers = {
        'X-Yandex-API-Key': key
    }
    response = requests.get(url, headers=headers)
    data = response.json()
    return data


def extract_weather_info(weather_data):
    dates = []
    avg_temperatures = []
    min_temperatures = []
    max_temperatures = []
    humidity = []
    wind_speed = []

    for forecast in weather_data['forecasts']:
        date = forecast['date']
        temp_avg = forecast['parts']['day']['temp_avg']
        temp_min = forecast['parts']['day']['temp_min']
        temp_max = forecast['parts']['day']['temp_max']
        hum = forecast['parts']['day']['humidity']
        wind = forecast['parts']['day']['wind_speed']

        dates.append(date)
        avg_temperatures.append(temp_avg)
        min_temperatures.append(temp_min)
        max_temperatures.append(temp_max)
        humidity.append(hum)
        wind_speed.append(wind)

    return dates, avg_temperatures, min_temperatures, max_temperatures, humidity, wind_speed

def plot_temperature_comparison(city_weather_info):
    plt.figure(figsize=(14, 7))

    for city, (dates, avg_temp, min_temp, max_temp, hum, wind) in city_weather_info.items():
        plt.plot(dates, avg_temp, marker='o', label=f'{city} Avg Temp')
        plt.fill_between(dates, min_temp, max_temp, alpha=0.2)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.title('Сравнение средней температуры по городам')
    plt.xlabel('Дата')
    plt.ylabel('Температура, °C')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f'data/temperature-{str(datetime.date.today().isoformat())}.png')
    plt.show()


def plot_humidity_and_wind(city_weather_info, city):
    dates, avg_temp, min_temp, max_temp, humidity, wind_speed = city_weather_info[city]

    fig, ax1 = plt.subplots(figsize=(14, 7))
    fig.subplots_adjust(hspace=8)
    
    color = 'tab:blue'
    ax1.set_xlabel('Дата')
    ax1.set_ylabel('Влажность (%)', color=color)
    ax1.plot(dates, humidity, color=color, marker='o', label='Влажность')
    ax1.tick_params(axis='y', labelcolor=color)

    ax2 = ax1.twinx()
    color = 'tab:green'
    ax2.set_ylabel('Скорость ветра (м/с)', color=color)
    ax2.plot(dates, wind_speed, color=color, marker='x', linestyle='--', label='Скорость ветра')
    ax2.tick_params(axis='y', labelcolor=color)

    fig.tight_layout()
    plt.title(f'Влажность и скорость ветра в {city}')
    plt.xticks(rotation=45)
    plt.grid(True)
    
    plt.savefig(f'data/wind-{str(datetime.date.today().isoformat())}.png')
    plt.show()
    
    
def main():
    key = Variable.get('weather')
    
    cities = {
        'Moscow': (55.7558, 37.6176),
        'Saint Petersburg': (59.9343, 30.3351),
        'Volgograd': (48.7000, 44.5166)
    }
    
    weather_data = {city: fetch_weather_data(key, lat, lon) for city, (lat, lon) in cities.items()}
    city_weather_info = {city: extract_weather_info(data) for city, data in weather_data.items()}

    # Сравнение средней температуры по городам
    plot_temperature_comparison(city_weather_info)

    # Влажность и скорость ветра для Москвы
    plot_humidity_and_wind(city_weather_info, 'Moscow')
        
        

with DAG(
    dag_id='weather',
    default_args=default_args,
    description='Test application to demonstrate how the API works',
    schedule_interval=None,
) as dag:
    weather_process = PythonOperator(
        task_id='weather_forecast',
        python_callable=main,
        dag=dag
    )
    
weather_process