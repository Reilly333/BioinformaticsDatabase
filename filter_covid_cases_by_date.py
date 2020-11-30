import datetime
import numpy
from pandas import read_csv, to_datetime


def filter_by_date_county_covid_cases(start_date=datetime.date(2020, 8, 31), end_date=datetime.date(2020, 10, 1),
                                      min_county_pop=100000):
    csv_dataframe = read_csv('raw_data/cases_and_deaths_by_county_timeseries.csv')

    # Convert our date columns into numpy.datetime64 datetypes
    csv_dataframe['date'] = to_datetime(csv_dataframe['date'], format='%Y-%m-%d')

    # Filter dataframe with date filters between start_date and end_date
    csv_dataframe = csv_dataframe[
        (csv_dataframe['date'] > numpy.datetime64(start_date))
        &
        (csv_dataframe['date'] < numpy.datetime64(end_date))
    ]

    # Filter dataframe with minimum population number
    csv_dataframe = csv_dataframe[(csv_dataframe['total_population'] > min_county_pop)]
    print(csv_dataframe['fips_code'])
    return csv_dataframe

if __name__ == '__main__':
    filter_by_date_county_covid_cases()


