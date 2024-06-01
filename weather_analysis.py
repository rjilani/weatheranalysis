import duckdb
import csv
import matplotlib.pyplot as plt
import os

city = 'Peshawar'
dir_path = './output'
plot_graph = True
save_top_ten_file = False
rm_data_file = True


def avearge_temp_year():
    con = duckdb.connect("data/daily_weather_0524.db")
    filename = f"{dir_path}/weather_records_{city}.csv"
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
    df = duckdb.sql(
        f"SELECT * FROM '{dir_path}/weather_records_{city}.csv' "
        r"order by Temperature desc limit 10").df()
    print(df.head(n=10))
    if save_top_ten_file:
        df.to_csv(f"{dir_path}/weather_records_hottest_{city}_top_10.csv", encoding='utf-8', index=False)
    if plot_graph:
        draw_plot(df, f"Top 10 Hottest years of {city}")


def draw_plot(df, title):
    plt.figure(figsize=(9, 6))
    plt.bar(x=df['Year'],
            height=df['Temperature'],
            color='midnightblue')
    plt.xticks(rotation=45)
    plt.title(title).set_color('midnightblue')
    plt.xlabel("Year").set_color('midnightblue')
    plt.ylabel("Temperature in Celsius").set_color('midnightblue')
    plt.savefig(f"./{dir_path}/{title}")


def top_ten_coldest_yr():
    df = duckdb.sql(
        f"SELECT * FROM '{dir_path}/weather_records_{city}.csv' "
        r"order by Temperature asc limit 10").df()
    print(df.head(n=10))
    if save_top_ten_file:
        df.to_csv(f"{dir_path}/weather_records_coldest_{city}_top_10.csv", encoding='utf-8', index=False)
    if plot_graph:
        draw_plot(df, f"Top 10 coldest years of {city}")


def remove_data_file():
    os.remove(f"{dir_path}/weather_records_{city}.csv")


if __name__ == '__main__':
    try:
        avearge_temp_year()
        top_ten_hottest_yr()
        top_ten_coldest_yr()
        if rm_data_file:
            remove_data_file()
    except Exception as e:
        print("An exception occurs: " + str(e))
        print(e)
