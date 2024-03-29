import copy
import datetime

from datetime import timedelta

import pandas as pd

from cognite.client.data_classes import TimeSeries
from prophet import Prophet


def create_and_save_time_series_data(client, data, ts_external_id, data_set_id):
    """Function to create the time series and save the TS data"""
    cdf_ts = client.time_series.retrieve(external_id=ts_external_id)
    if cdf_ts is None:
        ts = TimeSeries(external_id=ts_external_id, name=ts_external_id, data_set_id=data_set_id)
        client.time_series.create(ts)
        print(f"Created time series: {ts_external_id}")
    else:
        print(f"Existing Time Series: {ts_external_id}")
    data.columns = ["values"]
    dps = []
    for index, r in data.iterrows():
        dps = dps + [{"timestamp": r.name, "value": r["values"]}]
    client.datapoints.insert(datapoints=dps, external_id=ts_external_id)


def ts_forecast(df, cps=0.02):
    """Function to time series forecast"""
    df2 = copy.deepcopy(df)
    df2.columns = ["ds", "y"]
    print(df2.columns)
    m = Prophet(changepoint_prior_scale=cps)
    m.fit(df2)
    future = m.make_future_dataframe(periods=24 * 7, freq="H")
    future["cap"] = 0.8 * df2["y"].median()
    fcst = m.predict(future)
    fcst_df = fcst[["ds", "yhat", "cap", "yhat_lower", "yhat_upper"]].set_index("ds")
    return fcst_df


def save_data(client, fcst_df, df, ts_exid, data_set_id):
    create_and_save_time_series_data(client, df[["Measurement"]], f"{ts_exid}_Actual", data_set_id=data_set_id)
    create_and_save_time_series_data(client, fcst_df[["yhat"]], f"{ts_exid}_Forecast_Trend", data_set_id=data_set_id)
    create_and_save_time_series_data(
        client, fcst_df[["yhat_lower"]], f"{ts_exid}_Forecast_Lower", data_set_id=data_set_id
    )
    create_and_save_time_series_data(
        client, fcst_df[["yhat_upper"]], f"{ts_exid}_Forecast_Upper", data_set_id=data_set_id
    )
    create_and_save_time_series_data(client, fcst_df[["cap"]], f"{ts_exid}_Forecast_Cap", data_set_id=data_set_id)


def save_test_data(client, df_for_test, ts_exid, data_set_id):
    create_and_save_time_series_data(
        client, df_for_test[["Ground_Truth"]], f"{ts_exid}_Actual_for_Test", data_set_id=data_set_id
    )
    create_and_save_time_series_data(
        client, df_for_test[["Forecast"]], f"{ts_exid}_Forecast_Trend_for_Test", data_set_id=data_set_id
    )
    create_and_save_time_series_data(
        client, df_for_test[["Error"]], f"{ts_exid}_Forecast_Error_for_Test", data_set_id=data_set_id
    )
    create_and_save_time_series_data(
        client,
        df_for_test[["Absolute Error Percentage"]],
        f"{ts_exid}_Forecast__Absolute_Error_Percentage_for_Test",
        data_set_id=data_set_id,
    )


def handle(client, data=None, secrets=None, function_call_info=None):
    """Handler Function to be Run/Deployed for heat exchangers
    Args:
        client : Cognite Client (not needed, it's availble to it, when deployed)
        data : data needed by function
        secrets : Any secrets it needs
        function_call_info : any other information about function

    Returns:
        response : response or result from the function
    """
    pump_ts_extid_list = [
        "USA.ST.KONG.VIRT.012-PBA-6270A_Monitor_ActualHead-Numerical",
        "USA.TB.KONG.VIRT.PBE-6420_Monitor_ActualHead",
    ]

    data_set_id = 6870218523598358  # client.data_sets.retrieve(external_id="cognite_replicator_test").id
    column_names = ["Measurement"]
    # ts_exids = ["USA.ST.KONG.VIRT.005-CAE-5040A_Monitor_ActualPolytropicEfficiency"]
    start_date = datetime.datetime(2022, 6, 2)
    end_date = start_date + timedelta(days=45)
    for ts_exid in pump_ts_extid_list:
        print("Processing {}".format(ts_exid))
        df = client.datapoints.retrieve_dataframe(
            external_id=[ts_exid],
            aggregates=["average"],
            granularity="1h",
            start=start_date,
            end=end_date,
            include_aggregate_name=False,
        )
        df.columns = column_names
        # remove outlier. TO DO optimization
        df["Measurement"] = df["Measurement"].apply(lambda x: None if x == 0 else x)
        df.reset_index(inplace=True)
        # Forecast TS
        fcst_df = ts_forecast(df)

        # Save the Results as time series
        df.set_index(["index"], inplace=True)
        df.fillna(method="ffill", inplace=True)
        df.fillna(method="bfill", inplace=True)
        save_data(client, fcst_df, df, ts_exid, data_set_id)

        # retrieve test data
        start_date_test = end_date
        end_date_test = start_date_test + timedelta(days=7)
        print("Processing test data from {} to {}".format(start_date_test, end_date_test))
        df_test = client.datapoints.retrieve_dataframe(
            external_id=[ts_exid],
            aggregates=["average"],
            granularity="1h",
            start=start_date_test,
            end=end_date_test,
            include_aggregate_name=False,
        )
        df_test.columns = column_names
        df_predict = fcst_df[["yhat"]]

        # prepare test data for  dashbaord
        df_merged = pd.merge(df_test, df_predict, left_index=True, right_index=True)
        df_merged.columns = ["Ground_Truth", "Forecast"]
        df_merged["Error"] = df_merged["Forecast"] - df_merged["Ground_Truth"]
        df_merged["Absolute Error Percentage"] = 0
        for idx, row in df_merged.iterrows():
            gt = row["Ground_Truth"]
            if gt == 0:
                gt = 0.001
            error = row["Error"]
            absolute_error_percentage = round(abs(error / gt) * 100, 2)
            df_merged.at[idx, "Absolute Error Percentage"] = absolute_error_percentage
        df_merged.fillna(method="ffill", inplace=True)
        df_merged.fillna(method="bfill", inplace=True)
        save_test_data(client, df_merged, ts_exid, data_set_id)
    print("processing is done")
    return pump_ts_extid_list
