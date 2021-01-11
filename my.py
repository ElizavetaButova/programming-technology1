import requests
from sqlalchemy import create_engine, Table, Column, String, Float, MetaData
from sqlalchemy.sql import select


class WeatherProvider:
    def __init__(self, key):
        self.key = key

    def get_data(self):

        url = 'http://api.openweathermap.org/data/2.5/weather?q=Volgograd&appid=8a069a4288d80c01e9992fded22ed144'

        data = requests.get(url).json()
        return [
            {
                'Temp':data['main']['temp'],
                'Feels_like':data['main']['feels_like'],
                'Temp_min':data['main']['temp_min'],
                'Temp_max':data['main']['temp_max'],
                'Pressure':data['main']['pressure'],
                'Humidity':data['main']['humidity']
            }
            
        ]

def db_meth(str_conn, data_name):

    engine = create_engine(str_conn)
    metadata = MetaData()
    weather = Table(
        data_name,
        metadata,
        Column('Temp', Float),
        Column('Feels_like', Float),
        Column('Temp_min', Float),
        Column('Temp_max', Float),
        Column('Pressure', Float),
        Column('Humidity', Float)
    )
    return engine, metadata, weather

engine, metadata, weather = db_meth('sqlite:///today\'s_weather', 'weather')
metadata.create_all(engine)
c = engine.connect()

provider = WeatherProvider('8a069a4288d80c01e9992fded22ed144')
c.execute(weather.insert(), provider.get_data())

for row in c.execute(select([weather])):
    print(row)
