import pandas as pd
import datetime
from pathlib import Path


def get_csv_filter_write_csv(csv_data):
    """
    The result is a DataFrame where the "event_type" is either "Battles" or "Explosions/Remote violence". The
    first CSV file is also stored in the working directory after this function is executed.

    :param csv_data : a string - name of the dataset (or link_to the data set).
    :return: DataFrame.
    """
    attacks_data = pd.read_csv(csv_data)
    events = ["Battles", "Explosions/Remote violence"]
    filtered_df = attacks_data.loc[attacks_data['event_type'].isin(events)]
    filtered_df.to_csv("Battles_Explosions_remote-violence.csv")
    return filtered_df


def build_event_month_column(df):
    """
    Formats the date from "event_date" and store it in a newly added column "event_month".
    The "event_date" will not yet be erased.
    :param df: DataFrame
    :return: A DataFrame with an additional column "event_month"
    """
    form_date = pd.to_datetime(df['event_date'], errors='ignore')
    form_date = form_date.dt.strftime("%Y-%m")
    df.insert(1, "event_month", form_date)
    return df


def get_new_frame_from_date(filtered_df_0):
    """
    The output of build_event_month_column() will be scanned for the year and month that the user will input.
    The function will extract all the rows, where the wished month and year appear and store them into a new DataFrame
    which will be further treated. (A bit chaotic)
    :param filtered_df_0: DataFrame
    :return: DataFrame - and messages depending on the user's inputs()
    """

    months_choices = []
    for i in range(1, 13):
        months_choices.append(datetime.date(1, i, 1).strftime('%B'))

    filtered_df = build_event_month_column(filtered_df_0)

    print("Please enter a Year (yyyy)")
    y = input()
    while not y.isdecimal():
        print("Enter a proper year (in numbers)")
        y = input()

    print("Please enter a Month")
    m = input()
    if m.isdecimal():
        while int(m) >= 13 or int(m) <= 0:
            print("please enter a valid month")
            m = input()

        mm = months_choices[int(m) - 1]
        if int(m) in range(10, 13):
            month = str(y) + "-" + str(int(m))

        else:
            month = str(y) + "-0" + str(int(m))

        attack_by_month = filtered_df.loc[filtered_df['event_month'] == month]

    else:
        for i in range(len(months_choices)):
            if m.upper() in months_choices[i].upper():
                m = months_choices[i]
                mm = months_choices[i]

        month_year = str(m + " " + y)
        attack_by_month = filtered_df.loc[filtered_df['event_date'].str.contains(month_year, case=False)]

    if attack_by_month.size == 0:
        print(f"The CSV file is empty: either there have been no Battles or Explosions/Remote violence on {mm} "
              f"of {y}, or the year {y} is out of scope")
    else:
        destination = Path.cwd()
        print(f"The Data you acquired (for {mm}-{y}) is to be found in the following Directory:", destination)

    return attack_by_month  # DataFrame


def unify_actor_column(not_that_clean):
    """
    This function creates a new DataFrame where all the actors extracted from original columns "actor1", "actor2",
    "assoc_actor_1" and "assoc_actor_2". The "NA"-values will be ignored and the column "admin1" is renamed to "regions"
    :param not_that_clean: DataFrame
    :return: DataFrame
    """
    indx_lst = ["actor1", "actor2", "assoc_actor_1", "assoc_actor_2"]

    df_all_actors_raw = pd.DataFrame()

    for k in indx_lst:
        needed_columns = ["country", "event_month", k, "admin1"]
        df_by_actor = not_that_clean[needed_columns].rename(columns={k: "actor"})

        df_all_actors_raw = df_all_actors_raw.append(df_by_actor)

    df_all_actors_raw = df_all_actors_raw.dropna()
    df_all_actors_raw.rename(columns={"admin1": "regions"}, inplace=True)

    return df_all_actors_raw


def last_cleansing(raw_data):
    """
    This is the last step. The unify_actor_column() will be applied. This will erase the repeated rows and sum up the
    actors delivering lists of cities in the column "regions"
    :param raw_data: DataFrame
    :return: DataFrame
    """
    u_raw_data = unify_actor_column(raw_data)
    # print(u_raw_data)
    u_raw_data = u_raw_data.drop_duplicates()
    # print(u_raw_data)
    u_raw_data = u_raw_data.groupby(["country", "event_month", "actor"], as_index=False).agg({'regions': lambda x: list(x)})
    u_raw_data.drop(u_raw_data.columns[0], axis=1, inplace=True)

    return u_raw_data


if __name__ == "__main__":
    # data_source = "https://gist.githubusercontent.com/iwasawatower/3c358ac866e0718e4ff367398e0d590b/raw/" \
                  #"dbec3a44e66762cae3f39c3825a3dbc3f79718a0/sample_data.csv"

    data_source = "sample_data.csv"

    filtered_data = get_csv_filter_write_csv(data_source)

    month_filtered = get_new_frame_from_date(filtered_data)

    last_df = last_cleansing(month_filtered)

    last_df.to_csv("by_actor_and_region.csv", index=False)

    # print(new_column_filtered)
    # print(month_filtered)
    # print(all_actors)
    # print(last_df.applymap(type))
    # print(last_df)
