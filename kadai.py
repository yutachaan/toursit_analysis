import pandas as pd

h26_files = ["data/H26-{0}.xlsx".format(i) for i in range(1, 5)]
h27_files = ["data/H27-{0}.xlsx".format(i) for i in range(1, 5)]
h28_files = ["data/H28-{0}.xlsx".format(i) for i in range(1, 5)]
h29_files = ["data/H29-{0}.xlsx".format(i) for i in range(1, 5)]

def process_files1(files):
    '''sheet1と2の加工'''

    col_names = ["都道府県", "①_県内_宿泊", "①_県内_日帰り", "①_県外_宿泊", "①_県外_日帰り",
    "②_県内_宿泊", "②_県内_日帰り", "②_県外_宿泊", "②_県外_日帰り"]

    sheet1 = pd.DataFrame()

    for(i, file) in enumerate(files):
        temp = pd.read_excel(file, sheet_name=1, skiprows=6, skipfooter=1, usecols=range(9), names=col_names, index_col=0)
        temp = temp.stack()
        temp.index.rename("客層", level=1, inplace=True)
        temp.name = f'{i + 1}_観光'
        temp = temp.reset_index()
        if not sheet1.empty: sheet1 = pd.merge(sheet1, temp, on=["都道府県", "客層"], how="left")
        else: sheet1 = temp

    sheet2 = pd.DataFrame()

    for(i, file) in enumerate(files):
        temp = pd.read_excel(file, sheet_name=2, skiprows=6, skipfooter=1, usecols=range(9), names=col_names, index_col=0)
        temp = temp.stack()
        temp.index.rename("客層", level=1, inplace=True)
        temp.name = f'{i + 1}_ビジネス'
        temp = temp.reset_index()
        if not sheet2.empty: sheet2 = pd.merge(sheet2, temp, on=["都道府県", "客層"], how="left")
        else: sheet2 = temp

    df = pd.DataFrame()
    df = pd.merge(sheet1, sheet2, on=["都道府県", "客層"], how="left")
    df = df.dropna(how="any")
    for i in range(1, 5):
        df = df[(df[f'{i}_観光'] != "集計中") & (df[f'{i}_観光'] != "-")]
        df = df[(df[f'{i}_ビジネス'] != "集計中") & (df[f'{i}_ビジネス'] != "-")]
    df.insert(1, "種類", "①")
    df["種類"] = df["種類"].where(df["客層"].str.contains("①"), "②")
    df = df.drop(columns='客層')
    df.iloc[:, 2:10] = df.iloc[:, 2:10].astype(int)
    df = df.groupby(["都道府県", "種類"], as_index=False).sum()

    return df

def process_files2(files):
    '''sheet4と5の加工'''

    col_names = ["都道府県", "自然", "歴史・文化", "温泉・健康", "スポーツ・レク", "都市型観光", "その他", "イベント"]

    sheet4 = pd.DataFrame()

    for(i, file) in enumerate(files):
        temp = pd.read_excel(file, sheet_name=4, skiprows=6, skipfooter=1, usecols=[0, 3, 4, 5, 6, 7, 8, 10], names=col_names, index_col=0)
        temp = temp.stack()
        temp.index.rename("目的", level=1, inplace=True)
        temp.name = f'{i + 1}_地点数'
        temp = temp.reset_index()
        if not sheet4.empty: sheet4 = pd.merge(sheet4, temp, on=["都道府県", "目的"], how="left")
        else: sheet4 = temp

    sheet5 = pd.DataFrame()

    for(i, file) in enumerate(files):
        temp = pd.read_excel(file, sheet_name=5, skiprows=6, skipfooter=1, usecols=[0, 3, 4, 5, 6, 7, 8, 9], names=col_names, index_col=0)
        temp = temp.stack()
        temp.index.rename("目的", level=1, inplace=True)
        temp.name = f'{i + 1}_客数'
        temp = temp.reset_index()
        if not sheet5.empty: sheet5 = pd.merge(sheet5, temp, on=["都道府県", "目的"], how="left")
        else: sheet5 = temp

    df = pd.DataFrame()
    df = pd.merge(sheet4, sheet5, on=["都道府県", "目的"], how="left")
    df = df.dropna(how="any")
    df.iloc[:, 2:10] = df.iloc[:, 2:10].astype(int)

    return df

#sheet1&sheet2
h26_jp = process_files1(h26_files)
h27_jp = process_files1(h27_files)
h28_jp = process_files1(h28_files)
h29_jp = process_files1(h29_files)
h26_jp.to_csv("data_after/jp/h26.csv", index=False)
h27_jp.to_csv("data_after/jp/h27.csv", index=False)
h28_jp.to_csv("data_after/jp/h28.csv", index=False)
h29_jp.to_csv("data_after/jp/h29.csv", index=False)

#sheet4&sheet5
h26_resorts = process_files2(h26_files)
h27_resorts = process_files2(h27_files)
h28_resorts = process_files2(h28_files)
h29_resorts = process_files2(h29_files)
h26_resorts.to_csv("data_after/resorts/h26.csv", index=False)
h27_resorts.to_csv("data_after/resorts/h27.csv", index=False)
h28_resorts.to_csv("data_after/resorts/h28.csv", index=False)
h29_resorts.to_csv("data_after/resorts/h29.csv", index=False)
