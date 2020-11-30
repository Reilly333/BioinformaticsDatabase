SELECT avg_temperature_air_2m_f, city, state, covid_cases.date, total_population, new_cases, new_deaths FROM covid_cases INNER JOIN temp_data ON temp_data.fips_code = covid_cases.fips_code;
