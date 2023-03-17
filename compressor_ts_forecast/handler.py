import copy
import datetime

from datetime import timedelta

from cognite.client.data_classes import TimeSeries
from prophet import Prophet


def create_and_save_time_series_data(client, data, ts_external_id, data_set_id):
    """Function to create the time series and save the TS data"""
    cdf_ts = client.time_series.retrieve(external_id=ts_external_id)
    if cdf_ts is None:
        ts = TimeSeries(external_id=ts_external_id, name=ts_external_id, data_set_id=data_set_id)
        client.time_series.create(ts)
        print("Created time series")
    else:
        print("Existing Time Series")
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
    future["cap"] = 1.1 * df2["y"].mean()  #
    fcst = m.predict(future)
    fcst_df = fcst[["ds", "yhat", "cap", "yhat_lower", "yhat_upper"]].set_index("ds")
    return fcst_df


def handle(client, data=None, secrets=None, function_call_info=None):
    """Handler Function to be Run/Deployed
    Args:
        client : Cognite Client (not needed, it's availble to it, when deployed)
        data : data needed by function
        secrets : Any secrets it needs
        function_call_info : any other information about function

    Returns:
        response : response or result from the function
    """
    compressor_ts_extid_list = [
        "USA.ST.KONG.VIRT.005-CAE-5040A_Monitor_ActualPolytropicEfficiency-Numerical",
        "USA.ST.KONG.VIRT.005-CAE-5040A_Monitor_ActualPolytropicHead",
        "USA.ST.KONG.VIRT.005-CAE-5040A_Monitor_EfficiencyDeviation",
    ]

    data_set_id = 6870218523598358  # client.data_sets.retrieve(external_id="cognite_replicator_test").id
    column_names = ["Measurement"]
    # ts_exids = ["USA.ST.KONG.VIRT.005-CAE-5040A_Monitor_ActualPolytropicEfficiency"]
    start_date = datetime.datetime(2022, 6, 2)
    end_date = start_date + timedelta(days=45)
    for ts_exid in compressor_ts_extid_list:
        print("Processing {}".format(ts_exid))
        df = client.datapoints.retrieve_dataframe(
            external_id=[ts_exid],
            aggregates=["average"],
            granularity="1h",
            start=start_date,
            end=end_date,
            include_aggregate_name=False,
        )
        df.fillna(method="ffill", inplace=True)
        df.columns = column_names
        # remove outlier. TO DO optimization
        df["Measurement"] = df["Measurement"].apply(lambda x: None if x == 0 else x)
        df.reset_index(inplace=True)
        # Forecast TS
        fcst_df = ts_forecast(df)

        # Save the Results as time series
        df.set_index(["index"], inplace=True)
        create_and_save_time_series_data(client, df[["Measurement"]], f"{ts_exid}_Actual", data_set_id=data_set_id)
        create_and_save_time_series_data(
            client, fcst_df[["yhat"]], f"{ts_exid}_Forecast_Trend", data_set_id=data_set_id
        )
        create_and_save_time_series_data(
            client, fcst_df[["yhat_lower"]], f"{ts_exid}_Forecast_Lower", data_set_id=data_set_id
        )
        create_and_save_time_series_data(
            client, fcst_df[["yhat_upper"]], f"{ts_exid}_Forecast_Upper", data_set_id=data_set_id
        )
        create_and_save_time_series_data(client, fcst_df[["cap"]], f"{ts_exid}_Forecast_Cap", data_set_id=data_set_id)
        # Return the result as json
        # result = fcst_df[["yhat"]].to_json()
    print("processing is done")
    return compressor_ts_extid_list
