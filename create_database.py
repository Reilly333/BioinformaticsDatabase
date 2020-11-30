import sqlite3

conn = sqlite3.connect("covid_data.sqlite")
cur = conn.cursor()


def create_covid_data_cases(drop=True):
    print("Re-creating covid_cases table")
    if drop:
        cur.execute('DROP TABLE IF EXISTS covid_cases')
    cur.execute('''CREATE TABLE "covid_cases"(
                    "fips_code" TEXT NOT NULL,
                    "city" TEXT,
                    "state" TEXT,
                    "date" DATE NOT NULL,
                    "total_population" REAL,
                    "cum_cases" REAL,
                    "cum_cases_per_100000" REAL,
                    "cum_deaths" REAL,
                    "cum_deaths_per_100000" REAL,
                    "new_cases" REAL,
                    "new_deaths" REAL,
                    "new_cases_per_100000" REAL, 
                    "new_deaths_per_100000" REAL,
                    "new_cases_7_day_rolling_avg" REAL,
                    "new_deaths_7_day_rolling_avg" REAL,
                    PRIMARY KEY (fips_code, date)
                    )''')
    conn.commit()


def create_covid_data_temp(drop=True):
    print("Re-creating temp_data table")
    if drop:
        cur.execute('DROP TABLE IF EXISTS temp_data')
    cur.execute('''CREATE TABLE "temp_data"(
                "fips_code" TEXT NOT NULL,
                "country" TEXT,
                "date" DATE NOT NULL,
                "min_temperature_air_2m_f" REAL,
                "avg_temperature_air_2m_f" REAL,
                "max_temperature_air_2m_f" REAL,
                "min_humidity_relative_2m_pct" REAL,
                "avg_humidity_relative_2m_pct" REAL,
                "max_humidity_relative_2m_pct" REAL,
                "min_humidity_specific_2m_gpkg" REAL,
                "avg_humidity_specific_2m_gpkg" REAL,
                "max_humidity_specific_2m_gpkg" REAL,
                PRIMARY KEY (fips_code, date),
                FOREIGN KEY(fips_code) REFERENCES covid_cases(fips_code) 
                )''')
    conn.commit()


if __name__ == '__main__':
    create_covid_data_cases()
    create_covid_data_temp()
