import os
import pandas as pd
import plotly.express as px
from pandas.tseries.offsets import DateOffset

home_path = os.getcwd()

path_to_data = os.path.join(home_path, "src", "data")
path_to_save_synthetic_data = os.path.join(home_path, "src", "data", "ats_data_synthetic")


path_arkhangelsk_obl = os.path.join(path_to_data, "ats_data_real", "ARHENERG_ZONE1_E_PARHENER.csv")
path_amurskaya_obl = os.path.join(path_to_data, "ats_data_real", "DEKENERG_ZONE2_S_PAMURENE.csv")
path_evreyskaya_obl = os.path.join(path_to_data, "ats_data_real", "DEKENERG_ZONE2_S_PEVRAOBL.csv")

df_arkhangelsk_obl = pd.read_csv(path_arkhangelsk_obl)
df_amurskaya_obl = pd.read_csv(path_amurskaya_obl)
df_evreyskaya_obl = pd.read_csv(path_evreyskaya_obl)


for df in [df_arkhangelsk_obl, df_amurskaya_obl, df_evreyskaya_obl]:
    df['datetime'] = pd.to_datetime(df['datetime'])


def plot_vc_fact(df, title):
    fig = px.line(df, x='datetime', y='VC_факт', title=title)
    fig.show()

df_synthetics_arkhangelsk_obl = df_arkhangelsk_obl.copy()
df_synthetics_amurskaya_obl = df_amurskaya_obl.copy()
df_synthetics_evreyskaya_obl = df_evreyskaya_obl.copy()

df_synthetics_arkhangelsk_obl['datetime'] = df_synthetics_arkhangelsk_obl['datetime'] + DateOffset(years=10)
df_synthetics_amurskaya_obl['datetime'] = df_synthetics_amurskaya_obl['datetime'] + DateOffset(years=10)
df_synthetics_evreyskaya_obl['datetime'] = df_synthetics_evreyskaya_obl['datetime'] + DateOffset(years=10)


# plot_vc_fact(df_synthetics_arkhangelsk_obl, 'Архангельская обл.')
# plot_vc_fact(df_synthetics_amurskaya_obl, 'Амурская обл.')
# plot_vc_fact(df_synthetics_evreyskaya_obl, 'Еврейская обл.')



for df in [df_synthetics_arkhangelsk_obl, df_synthetics_amurskaya_obl, df_synthetics_evreyskaya_obl]:
    if 'Unnamed: 0' in df.columns:
        df.drop(columns=['Unnamed: 0'], inplace=True)

df_synthetics_arkhangelsk_obl.to_csv(
    os.path.join(path_to_save_synthetic_data, "arkhangelsk_obl_synthetic.csv"), index=False
)
df_synthetics_amurskaya_obl.to_csv(
    os.path.join(path_to_save_synthetic_data, "amurskaya_obl_synthetic.csv"), index=False
)
df_synthetics_evreyskaya_obl.to_csv(
    os.path.join(path_to_save_synthetic_data, "evreyskaya_obl_synthetic.csv"), index=False
)
