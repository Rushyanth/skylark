import pandas as pd
from shapely.geometry import Point, shape

from flask import Flask
from flask import render_template, request
import json
import csv
import datetime
from geopy import geocoders

data_path = './input/'
n_samples = 30000


def get_age_segment(age):
    if age <= 22:
        return '22-'
    elif age <= 26:
        return '23-26'
    elif age <= 28:
        return '27-28'
    elif age <= 32:
        return '29-32'
    elif age <= 38:
        return '33-38'
    else:
        return '39+'


def get_location(longitude, latitude, provinces_json):

    point = Point(longitude, latitude)

    for record in provinces_json['features']:
        polygon = shape(record['geometry'])
        if polygon.contains(point):
            return record['properties']['name']
    return 'other'


with open(data_path + '/geojson/china_provinces_en.json') as data_file:
    provinces_json = json.load(data_file)

app = Flask(__name__)


@app.route("/", methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        device_id = request.form['device_id']
        gender = request.form['gender']
        age = request.form['age']
        city = request.form['city']
        phone_brand = request.form['phone_brand']
        event_id = 0
        with open(data_path + 'events.csv', 'r') as f:
            for row in reversed(list(csv.reader(f))):
                event_id = int(row[0]) + 1
                break
        with open(data_path + 'gender_age_train.csv', 'a') as newFile:
            newFileWriter = csv.writer(newFile)
            newFileWriter.writerow([device_id, gender, age])
        timestamp = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        gn = geocoders.GeoNames(username="Rushyanthreddy")
        location = gn.geocode(city)
        print(timestamp)
        print(location.latitude)
        print(location.longitude)
        with open(data_path + 'events.csv', 'a') as newFile:
            newFileWriter = csv.writer(newFile)
            newFileWriter.writerow(
                [event_id, device_id, timestamp, location.longitude,
                 location.latitude])
        with open(data_path + 'phone_brand_device_model.csv', 'a') as newFile:
            newFileWriter = csv.writer(newFile)
            newFileWriter.writerow([device_id, phone_brand])
    return render_template("index.html")


@app.route("/data")
def get_data():
    gen_age_tr = pd.read_csv(data_path + 'gender_age_train.csv')
    ev = pd.read_csv(data_path + 'events.csv')
    ph_br_dev_model = pd.read_csv(data_path + 'phone_brand_device_model.csv')

    df = gen_age_tr.merge(ev, how='inner', on='device_id')
    df = df.merge(ph_br_dev_model, how='inner', on='device_id')
    print(df.shape)
    # # Get n_samples records
    # df = df[df['longitude'] != 0].sample(n=n_samples)

    top_10_brands_en = {'华为': 'Huawei', '小米': 'Xiaomi', '三星': 'Samsung', 'vivo': 'vivo', 'OPPO': 'OPPO',
                        '魅族': 'Meizu', '酷派': 'Coolpad', '乐视': 'LeEco', '联想': 'Lenovo', 'HTC': 'HTC'}

    df['phone_brand_en'] = df['phone_brand'].apply(lambda phone_brand: top_10_brands_en[phone_brand]
                                                   if (phone_brand in top_10_brands_en) else 'Other')

    df['age_segment'] = df['age'].apply(lambda age: get_age_segment(age))

    df['location'] = df.apply(lambda row: get_location(
        row['longitude'], row['latitude'], provinces_json), axis=1)

    cols_to_keep = ['timestamp', 'longitude', 'latitude',
                    'phone_brand_en', 'gender', 'age_segment', 'location']
    df_clean = df[cols_to_keep].dropna()

    return df_clean.to_json(orient='records')


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=5000, debug=True)
