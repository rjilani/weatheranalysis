import duckdb
import csv

city = 'Austin'
dir_path = 'C:/Users/xxxx/Documents/Projects/PythonProject/weatheranalysis/output'

def avearge_temp_year():
    con = duckdb.connect("data/daily_weather_0524.db")
    filename = f"./output/weather_records_{city}.csv"
    x = range(1900, 2024)
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Year", "Temperature"])
        for n in x:
            temp = con.sql(
                f"select avg(avg_temp_c) as avg_temp from daily_weather where city_name = '{city}' and date >= '{n}-01-01 00:00:00.000' "
                f"and date <= '{n}-12-31 00:00:00.000'").fetchone()[0]
            if temp is not None:
                print(n)
                print(temp)
                writer.writerow([n, temp])
    con.close()


def top_ten_hottest_yr():
    global df
    df = duckdb.sql(
        f"SELECT * FROM '{dir_path}/weather_records_{city}.csv' "
        r"order by Temperature desc limit 10").df()
    print(df.head(n=10))
    df.to_csv(f"./output/weather_records_{city}_top10.csv", encoding='utf-8', index=False)


if __name__ == '__main__':
    try:
        avearge_temp_year()
        top_ten_hottest_yr()
    except Exception as e:
        print("An exception occurs: " + str(e))
        print(e)
