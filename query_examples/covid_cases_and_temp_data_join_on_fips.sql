#Joining tables on fips_codes
SELECT avg_temperature_air_2m_f, city, state, covid_cases.date, total_population, new_cases, new_deaths FROM covid_cases INNER JOIN temp_data ON temp_data.fips_code = covid_cases.fips_code;
hnji76

#find average of covid range of new covid cases#
SELECT fips_code, new_cases FROM covid_cases WHERE new_cases >= 50;

#Finding the counties with the coldest weather by fips_code

SELECT fips_code, min_temperature_air_2m_f FROM temp_data WHERE min_temperature_air_2m_f <= 50;

#Finding fips_codes that have higher number of covid_cases with lower temperature

SELECT min_temperature_air_2m_f, covid_cases.date, total_population, new_cases, new_deaths FROM covid_cases INNER JOIN temp_data ON temp_data.fips_code = covid_cases.fips_code WHERE new_cases >= 50 and min_temperature_air_2m_f <= 50;

#Finding fips_codes with high new covid cases that exceeds avg_humidity range

SELECT avg_humidity_specific_2m_gpkg, covid_cases.date, total_population, new_cases FROM covid_cases INNER JOIN temp_data ON temp_data.fips_code = covid_cases.fips_code WHERE new_cases >= 50 and avg_humidity_specific_2m_gpkg > 15 or avg_humidity_specific_2m_gpkg < 11;


