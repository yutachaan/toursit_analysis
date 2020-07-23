import pandas as pd

#データの読み込み
h26_files = ["data/H26-{0}.xlsx".format(i) for i in range(1, 5)]
h27_files = ["data/H27-{0}.xlsx".format(i) for i in range(1, 5)]
h28_files = ["data/H28-{0}.xlsx".format(i) for i in range(1, 5)]
h29_files = ["data/H29-{0}.xlsx".format(i) for i in range(1, 5)]

def treat_files(files, sheet):
    """sheet1, 2, 4, 5の加工"""

    df = pd.DataFrame()

    if sheet == 1 or sheet == 2:
        col_names = ["都道府県", "①_県内_宿泊", "①_県内_日帰り", "①_県外_宿泊", "①_県外_日帰り",
        "②_県内_宿泊", "②_県内_日帰り", "②_県外_宿泊", "②_県外_日帰り"]
        use_cols = range(9)
        name_1 = "客層"
    elif sheet == 4 or sheet == 5:
        if sheet == 4:
            use_cols = [0, 3, 4, 5, 6, 7, 8, 10]
            name_1 = "観光地数・イベント数"
        else:
            use_cols = [0, 3, 4, 5, 6, 7, 8, 9]
            name_1 = "観光地・イベント入込客数"
        col_names = ["都道府県", "自然", "歴史・文化", "温泉・健康", "スポーツ・レク", "都市型観光", "その他", "イベント"]

    for(i, file) in enumerate(files):
        temp = pd.read_excel(file, sheet_name=sheet, skiprows=6, skipfooter=1, usecols=use_cols, names=col_names, index_col=0)
        temp = temp.stack()
        temp.index.rename(name_1, level=1, inplace=True)
        temp.name = i + 1
        temp = temp.reset_index()
        if not df.empty: df = pd.merge(df, temp, on=["都道府県", name_1], how="left")
        else: df = temp

    df = df.dropna(how="any")

    if sheet == 1 or sheet == 2:
        for i in range(1, 5):
            df = df[(df[i] != "集計中") & (df[i] != "-")]
        df.insert(1, "種類", "②")
        df["種類"] = df["種類"].where(df["客層"].str.contains("②"), "①")

    df[[1, 2, 3, 4]] = df[[1, 2, 3, 4]].astype(int)

    if sheet == 1 or sheet == 2:
        df = df.groupby(["都道府県", "種類"], as_index=False).sum()
        df = df.sort_values("都道府県")

    return df

#sheet1
h26_jp_tour = treat_files(h26_files, 1)
h27_jp_tour = treat_files(h27_files, 1)
h28_jp_tour = treat_files(h28_files, 1)
h29_jp_tour = treat_files(h29_files, 1)

h26_jp_tour.to_csv("data_treat/jp_tour/h26.csv", index=False)
h27_jp_tour.to_csv("data_treat/jp_tour/h27.csv", index=False)
h28_jp_tour.to_csv("data_treat/jp_tour/h28.csv", index=False)
h29_jp_tour.to_csv("data_treat/jp_tour/h29.csv", index=False)

#sheet2
h26_jp_business = treat_files(h26_files, 2)
h27_jp_business = treat_files(h27_files, 2)
h28_jp_business = treat_files(h28_files, 2)
h29_jp_business = treat_files(h29_files, 2)

h26_jp_business.to_csv("data_treat/jp_business/h26.csv", index=False)
h27_jp_business.to_csv("data_treat/jp_business/h27.csv", index=False)
h28_jp_business.to_csv("data_treat/jp_business/h28.csv", index=False)
h29_jp_business.to_csv("data_treat/jp_business/h29.csv", index=False)

#sheet4
h26_resorts_events = treat_files(h26_files, 4)
h27_resorts_events = treat_files(h27_files, 4)
h28_resorts_events = treat_files(h28_files, 4)
h29_resorts_events = treat_files(h29_files, 4)

h26_resorts_events.to_csv("data_treat/resorts_events/h26.csv", index=False)
h27_resorts_events.to_csv("data_treat/resorts_events/h27.csv", index=False)
h28_resorts_events.to_csv("data_treat/resorts_events/h28.csv", index=False)
h29_resorts_events.to_csv("data_treat/resorts_events/h29.csv", index=False)

#sheet5
h26_resorts_events_tourist = treat_files(h26_files, 5)
h27_resorts_events_tourist = treat_files(h27_files, 5)
h28_resorts_events_tourist = treat_files(h28_files, 5)
h29_resorts_events_tourist = treat_files(h29_files, 5)

h26_resorts_events_tourist.to_csv("data_treat/resorts_events_tourist/h26.csv", index=False)
h27_resorts_events_tourist.to_csv("data_treat/resorts_events_tourist/h27.csv", index=False)
h28_resorts_events_tourist.to_csv("data_treat/resorts_events_tourist/h28.csv", index=False)
h29_resorts_events_tourist.to_csv("data_treat/resorts_events_tourist/h29.csv", index=False)
