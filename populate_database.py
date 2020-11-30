import os
import csv
import sqlite3
from datetime import datetime

from progressbar import ProgressBar

conn = sqlite3.connect("covid_data.sqlite")
cur = conn.cursor()


def covid_temperature_string_to_date(date_value):
    try:
        my_date_obj = datetime.strptime(date_value, '%m/%d/%Y')
    except ValueError:
        my_date_obj = datetime.strptime(date_value, '%Y-%m-%d')
    return my_date_obj


def covid_cases_string_to_date(date_value):
    my_date_obj = datetime.strptime(date_value, '%Y-%m-%d')
    return my_date_obj


def add_covid_case_data_from_file(fp):
    with open(fp) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        row_count = sum(1 for row in csv_file)
        csv_file.seek(0)
        next(csv_reader)
        pb = ProgressBar(maxval=row_count)
        pb.start()
        for i, row in enumerate(csv_reader):
            _, _, fips_code, city, state, date, total_population, cum_cases, cum_cases_per_100000, cum_deaths, \
                cum_deaths_per_100000, new_cases, new_deaths, new_cases_per_100000, new_deaths_per_100000, \
                new_cases_7_day_rolling_avg, new_deaths_7_day_rolling_avg = row
            pb.update(i)
            if not fips_code:
                continue

            city = str(city)
            state = str(state)
            date = covid_cases_string_to_date(date)
            if not total_population:
                total_population = 0
            else:
                total_population = float(total_population)
            if not cum_cases:
                cum_cases = 0
            else:
                cum_cases = float(cum_cases)
            if not cum_cases_per_100000:
                cum_cases_per_100000 = 0
            else:
                cum_cases_per_100000 = float(cum_cases_per_100000)
            if not cum_deaths:
                cum_deaths = 0
            else:
                cum_deaths = float(cum_deaths)
            if not cum_deaths_per_100000:
                cum_deaths_per_100000 = 0
            else:
                cum_deaths_per_100000 = float(cum_deaths_per_100000)

            if not new_cases:
                new_cases = 0
            else:
                new_cases = float(new_cases)
            if not new_deaths:
                new_deaths = 0
            else:
                new_deaths = float(new_deaths)

            if not new_cases_per_100000:
                new_cases_per_100000 = 0
            else:
                new_cases_per_100000 = float(new_cases_per_100000)

            if not new_deaths_per_100000:
                new_deaths_per_100000 = 0
            else:
                new_deaths_per_100000 = float(new_deaths_per_100000)

            if not new_cases_7_day_rolling_avg:
                new_cases_7_day_rolling_avg = 0
            else:
                new_cases_7_day_rolling_avg = float(new_cases_7_day_rolling_avg)

            if not new_deaths_7_day_rolling_avg:
                new_deaths_7_day_rolling_avg = 0
            else:
                new_deaths_7_day_rolling_avg = float(new_deaths_7_day_rolling_avg)

            # print(fips_code, city, state, date, total_population, cum_cases, cum_cases_per_100000, cum_deaths,
            # cum_deaths_per_100000, new_cases, new_deaths, new_cases_per_100000, new_deaths_per_100000,
            # new_cases_7_day_rolling_avg, new_deaths_7_day_rolling_avg)
            try:
                cur.execute("INSERT INTO covid_cases("
                            "fips_code,"
                            "city,"
                            "state,"
                            "date,"
                            "total_population,"
                            "cum_cases,"
                            "cum_cases_per_100000,"
                            "cum_deaths,"
                            "cum_deaths_per_100000,"
                            "new_cases,"
                            "new_deaths,"
                            "new_cases_per_100000,"
                            "new_deaths_per_100000,"
                            "new_cases_7_day_rolling_avg,"
                            "new_deaths_7_day_rolling_avg) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (fips_code,
                             city,
                             state,
                             date,
                             total_population,
                             cum_cases,
                             cum_cases_per_100000,
                             cum_deaths,
                             cum_deaths_per_100000,
                             new_cases,
                             new_deaths,
                             new_cases_per_100000,
                             new_deaths_per_100000,
                             new_cases_7_day_rolling_avg,
                             new_deaths_7_day_rolling_avg))
            except sqlite3.IntegrityError:
                print('Skipping inserting {}, {} as it already exists.'.format(fips_code, date))
        conn.commit()


def add_covid_temperature_data_from_file(fp):
    with open(fp) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        next(csv_reader)
        for i, row in enumerate(csv_reader):
            fips_code, country, date, min_temperature_air_2m_f, avg_temperature_air_2m_f, max_temperature_air_2m_f, \
                min_humidity_relative_2m_pct, avg_humidity_relative_2m_pct, max_humidity_relative_2m_pct, \
                min_humidity_specific_2m_gpkg, avg_humidity_specific_2m_gpkg, max_humidity_specific_2m_gpkg = row
            if not fips_code:
                continue

            country = str(country)

            date = covid_temperature_string_to_date(date)
            if not min_temperature_air_2m_f:
                continue

            if not avg_temperature_air_2m_f:
                continue

            if not max_temperature_air_2m_f:
                continue

            if not min_humidity_relative_2m_pct:
                continue

            if not avg_humidity_relative_2m_pct:
                continue

            if not max_humidity_relative_2m_pct:
                continue

            if not min_humidity_specific_2m_gpkg:
                continue

            if not avg_humidity_specific_2m_gpkg:
                continue

            if not max_humidity_specific_2m_gpkg:
                continue

            # print(fips_code, city, state, date, total_population, cum_cases, cum_cases_per_100000, cum_deaths,
            # cum_deaths_per_100000, new_cases, new_deaths, new_cases_per_100000, new_deaths_per_100000,
            # new_cases_7_day_rolling_avg, new_deaths_7_day_rolling_avg)
            try:
                cur.execute("INSERT INTO temp_data("
                            "fips_code,"
                            "country,"
                            "date,"
                            "min_temperature_air_2m_f,"
                            "avg_temperature_air_2m_f,"
                            "max_temperature_air_2m_f,"
                            "min_humidity_relative_2m_pct,"
                            "avg_humidity_relative_2m_pct,"
                            "max_humidity_relative_2m_pct,"
                            "min_humidity_specific_2m_gpkg,"
                            "avg_humidity_specific_2m_gpkg,"
                            "max_humidity_specific_2m_gpkg) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                            (fips_code,
                             country,
                             date,
                             min_temperature_air_2m_f,
                             avg_temperature_air_2m_f,
                             max_temperature_air_2m_f,
                             min_humidity_relative_2m_pct,
                             avg_humidity_relative_2m_pct,
                             max_humidity_relative_2m_pct,
                             min_humidity_specific_2m_gpkg,
                             avg_humidity_specific_2m_gpkg,
                             max_humidity_specific_2m_gpkg))
            except sqlite3.IntegrityError:
                print('Skipping inserting {}, {} as it already exists.'.format(fips_code, date))
        conn.commit()


if __name__ == '__main__':
    pb = ProgressBar()
    print('Adding Case Data.')
    add_covid_case_data_from_file(os.path.join('raw_data', 'cases_and_deaths_by_county_timeseries.csv'))

    print('Adding Weather Data.')
    for fp in pb(os.listdir('raw_data/weather_data')):
        add_covid_temperature_data_from_file(os.path.join('raw_data', 'weather_data', fp))
