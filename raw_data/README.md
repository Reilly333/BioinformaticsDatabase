### Dataset Sources

- [cases_and_deaths_by_county_timeseries.csv](https://data.world/associatedpress/johns-hopkins-coronavirus-case-tracker/workspace/file?filename=2_cases_and_deaths_by_county_timeseries.csv)
- [weather_data (weathersource.com)](https://console.cloud.google.com/marketplace/product/gcp-public-data-weather-source/weathersource-covid19?filter=category:covid19&q=weathersource&id=88b8d575-e1cd-48ec-98d0-8fdf2bbddd5f)

### Google CLoud Query Example

``` sql
SELECT * FROM `bigquery-public-data.covid19_weathersource_com.county_day_history` WHERE date > "2020-07-03" and date < "2020-07-06" LIMIT 50000
```