import pandas as pd
from datetime import datetime
import numpy as np
import regex as re

import functions
from functions import *

def generate_template(df):
    df.extract_unit_num_pickup()
    df.extract_unit_num_dropoff()
    df.extract_postal_code_from_pickup_address()
    df.extract_postal_code_from_dropoff_address()
    df.normalize_pickup_postal_code()
    df.normalize_dropoff_postal_code()
    df.convert_earliest_dropoff_time()
    df.convert_latest_dropoff_time()
    df.convert_earliest_pickup_time()
    df.convert_latest_pickup_time()
    df.convert_phone_nos_all()
    return df.df_new

def prepare_file_assurity(df):
    file = File(df)

    #xxx.duplicate_service_times_all("xxx")
    file.rename_columns_general({
                                            "ID" : "id",
                                            "Customer Name" : "name",
                                            "Dropoff Address" : "dropoff address",
                                            "Dropoff Unit Number" : "dropoff unit number",
                                            "Dropoff Postal Code" : "dropoff postal code",
                                            "Earliest dropoff time'" : "earliest dropoff time",
                                            "Latest dropoff time" : "latest dropoff time",
                                            "Pickup Address" : "pickup address",
                                            "Pickup Unit Number" : "pickup unit number",
                                            "Pickup Postal Code" : "pickup postal code",
                                            "Earliest pickup time" : "earliest pickup time",
                                            "Latest pickup time" : "latest pickup time",
                                            #"xxx" : "demand type",
                                            "Demand Load" : "demand load",
                                            "Remarks" : "remarks",
                                            "Client's Contact Number" : "phone number",
                                            #"xxx" : "pickup service time",
                                            #"xxx" : "dropoff service time"
                                                                            })
    file.remove_nil_rows()
    file.set_row_values_general({
                                            #"id" : "xxx",
                                            #"name" : "xxx",
                                            #"dropoff address" : "xxx",
                                            #"dropoff unit number" : "xxx",
                                            #"dropoff postal code" : "xxx",
                                            #"earliest dropoff time" : "xxx",
                                            #"latest dropoff time" : "xxx",
                                            #"pickup address" : "xxx",
                                            #"pickup unit number" : "xxx",
                                            #"pickup postal code" : "xxx",
                                            #"earliest pickup time" : "xxx",
                                            #"latest pickup time" : "xxx",
                                            #"demand type" : "xxx",
                                            #"demand load" : "xxx",
                                            #"remarks" : "xxx",
                                            #"phone number" : "xxx",
                                            "pickup service time" : 0,
                                            "dropoff service time" : 480})
    generate_template(file)
    return file.df_new


def prepare_file_interconntinental_hotel(df):
    file = File(df)
    #xxx.duplicate_service_times_all("xxx")
    file.rename_columns_general({
                                                            #"xxx" : "id",
                                                            "Customer Name" : "name",
                                                            "Dropoff Address" : "dropoff address",
                                                            #"xxx" : "dropoff unit number",
                                                            "Dropoff Postal Code" : "dropoff postal code",
                                                            "Earliest dropoff time (24 hrs format)" : "earliest dropoff time",
                                                            "Latest dropoff time (24hrs format)" : "latest dropoff time",
                                                            "Pickup Address (warehouse or hotel)" : "pickup address",
                                                            #"xxx" : "pickup unit number",
                                                            "Pickup Postal Code" : "pickup postal code",
                                                            "Earliest pickup time" : "earliest pickup time",
                                                            "Latest pickup time" : "latest pickup time",
                                                            #"xxx" : "demand type",
                                                            #"xxx" : "demand load",
                                                            "Remarks (any other special request)" : "remarks",
                                                            "Client's Contact Number" : "phone number",
                                                            #"xxx" : "pickup service time",
                                                            #"xxx" : "dropoff service time"
                                                                                            })
    file.remove_nil_rows()
    file.set_row_values_general({
                                                            #"id" : "xxx",
                                                            #"name" : "xxx",
                                                            #"dropoff address" : "xxx",
                                                            #"dropoff unit number" : "xxx",
                                                            #"dropoff postal code" : "xxx",
                                                            #"earliest dropoff time" : "xxx",
                                                            #"latest dropoff time" : "xxx",
                                                            #"pickup address" : "xxx",
                                                            #"pickup unit number" : "xxx",
                                                            #"pickup postal code" : "xxx",
                                                            #"earliest pickup time" : "xxx",
                                                            #"latest pickup time" : "xxx",
                                                            #"demand type" : "xxx",
                                                            #"demand load" : "xxx",
                                                            #"remarks" : "xxx",
                                                            #"phone number" : "xxx",
                                                            "pickup service time" : 0,
                                                            "dropoff service time" : 480})
    generate_template(file)
    return file.df_new

def prepare_file_shangrila_hotel(df):
    file = File(df)
    file.duplicate_service_times_all("Delivery Timings")
    file.rename_columns_general({
                                                    #"xxx" : "id",
                                                    "Name" : "name",
                                                    "Address" : "dropoff address",
                                                    "Unit Number" : "dropoff unit number",
                                                    "Postal Code" : "dropoff postal code",
                                                    #"xxx" : "earliest dropoff time",
                                                    #"xxx" : "latest dropoff time",
                                                    #"xxx" : "pickup address",
                                                    #"xxx" : "pickup unit number",
                                                    #"xxx" : "pickup postal code",
                                                    #"xxx" : "earliest pickup time",
                                                    #"xxx" : "latest pickup time",
                                                    #"xxx" : "demand type",
                                                    #"xxx" : "demand load",
                                                    #"xxx" : "remarks",
                                                    "Contact" : "phone number",
                                                    #"xxx" : "pickup service time",
                                                    #"xxx" : "dropoff service time"
                                                                                    })
    file.remove_nil_rows()
    
    file.set_row_values_general({
                                                    #"id" : "xxx",
                                                    #"name" : "xxx",
                                                    #"dropoff address" : "xxx",
                                                    #"dropoff unit number" : "xxx",
                                                    #"dropoff postal code" : "xxx",
                                                    #"earliest dropoff time" : "xxx",
                                                    #"latest dropoff time" : "xxx",
                                                    #"pickup address" : "xxx",
                                                    #"pickup unit number" : "xxx",
                                                    #"pickup postal code" : "xxx",
                                                    #"earliest pickup time" : "xxx",
                                                    #"latest pickup time" : "xxx",
                                                    #"demand type" : "xxx",
                                                    #"demand load" : "xxx",
                                                    #"remarks" : "xxx",
                                                    #"phone number" : "xxx",
                                                    "pickup service time" : 0,
                                                    "dropoff service time" : 480})
    generate_template(file)
    return file.df_new

def prepare_file_herbalife(df):
    file = File(df)
    #xxx.duplicate_service_times_all("xxx")
    file.rename_columns_general({
                                        #"xxx" : "id",
                                        "Name" : "name",
                                        "Address" : "dropoff address",
                                        #"xxx" : "dropoff unit number",
                                        "Postal Code" : "dropoff postal code",
                                        #"xxx" : "earliest dropoff time",
                                        #"xxx" : "latest dropoff time",
                                        #"xxx" : "pickup address",
                                        #"xxx" : "pickup unit number",
                                        #"xxx" : "pickup postal code",
                                        #"xxx" : "earliest pickup time",
                                        #"xxx" : "latest pickup time",
                                        #"xxx" : "demand type",
                                        #"xxx" : "demand load",
                                        #"xxx" : "remarks",
                                        #"xxx" : "phone number",
                                        #"xxx" : "pickup service time",
                                        #"xxx" : "dropoff service time"
                                                                        })
    file.remove_nil_rows()
    file.set_row_values_general({
                                        #"id" : "xxx",
                                        #"name" : "xxx",
                                        #"dropoff address" : "xxx",
                                        #"dropoff unit number" : "xxx",
                                        #"dropoff postal code" : "xxx",
                                        #"earliest dropoff time" : "xxx",
                                        #"latest dropoff time" : "xxx",
                                        #"pickup address" : "xxx",
                                        #"pickup unit number" : "xxx",
                                        #"pickup postal code" : "xxx",
                                        #"earliest pickup time" : "xxx",
                                        #"latest pickup time" : "xxx",
                                        #"demand type" : "xxx",
                                        #"demand load" : "xxx",
                                        #"remarks" : "xxx",
                                        #"phone number" : "xxx",
                                        "pickup service time" : 0,
                                        "dropoff service time" : 480})
    generate_template(file)
    return file.df_new

def prepare_file_wholefish(df):
    file = File(df)
    #xxx.duplicate_service_times_all("xxx")
    file.rename_columns_general({
                                        #"xxx" : "id",
                                        "Recipient Name" : "name",
                                        "Delivery Address" : "dropoff address",
                                        #"xxx" : "dropoff unit number",
                                        "Postal Code" : "dropoff postal code",
                                        "Delivery Start Time" : "earliest dropoff time",
                                        #"xxx" : "latest dropoff time",
                                        #"xxx" : "pickup address",
                                        #"xxx" : "pickup unit number",
                                        #"xxx" : "pickup postal code",
                                        "Delivery End Time" : "earliest pickup time",
                                        #"xxx" : "latest pickup time",
                                        #"xxx" : "demand type",
                                        #"xxx" : "demand load",
                                        "Customer Remarks" : "remarks",
                                        "Recipient Phone" : "phone number",
                                        #"xxx" : "pickup service time",
                                        #"xxx" : "dropoff service time"
                                                                        })
    file.remove_nil_rows()
    file.set_row_values_general({
                                        #"id" : "xxx",
                                        #"name" : "xxx",
                                        #"dropoff address" : "xxx",
                                        #"dropoff unit number" : "xxx",
                                        #"dropoff postal code" : "xxx",
                                        #"earliest dropoff time" : "xxx",
                                        #"latest dropoff time" : "xxx",
                                        #"pickup address" : "xxx",
                                        #"pickup unit number" : "xxx",
                                        #"pickup postal code" : "xxx",
                                        #"earliest pickup time" : "xxx",
                                        #"latest pickup time" : "xxx",
                                        #"demand type" : "xxx",
                                        #"demand load" : "xxx",
                                        #"remarks" : "xxx",
                                        #"phone number" : "xxx",
                                        "pickup service time" : 0,
                                        "dropoff service time" : 480})
    file.extract_unit_num_pickup()
    file.extract_unit_num_dropoff()
    file.extract_postal_code_from_pickup_address()
    file.extract_postal_code_from_dropoff_address()
    file.normalize_pickup_postal_code()
    file.normalize_dropoff_postal_code()
    file.convert_earliest_dropoff_time()
    file.convert_latest_dropoff_time()
    file.convert_earliest_pickup_time()
    file.convert_latest_pickup_time()
    file.convert_phone_nos_all()
    generate_template(file)
    return file.df_new

def prepare_file_ihg_1008_to_1508(df):
    file = File(df)
    #xxx.duplicate_service_times_all("xxx")
    file.rename_columns_general({
                                        #"xxx" : "id",
                                        "Customer Name" : "name",
                                        "Dropoff Address" : "dropoff address",
                                        #"xxx" : "dropoff unit number",
                                        "Dropoff Postal Code" : "dropoff postal code",
                                        "Earliest dropoff time (24 hrs format)" : "earliest dropoff time",
                                        "Latest dropoff time (24hrs format)" : "latest dropoff time",
                                        "Pickup Address (warehouse or hotel)" : "pickup address",
                                        #"xxx" : "pickup unit number",
                                        "Pickup Postal Code" : "pickup postal code",
                                        "Earliest pickup time" : "earliest pickup time",
                                        "Latest pickup time" : "latest pickup time",
                                        #"xxx" : "demand type",
                                        #"xxx" : "demand load",
                                        "Remarks (any other special request)" : "remarks",
                                        "Client's Contact Number" : "phone number",
                                        #"xxx" : "pickup service time",
                                        #"xxx" : "dropoff service time"
                                                                        })
    file.remove_nil_rows()
    file.set_row_values_general({
                                        #"id" : "xxx",
                                        #"name" : "xxx",
                                        #"dropoff address" : "xxx",
                                        #"dropoff unit number" : "xxx",
                                        #"dropoff postal code" : "xxx",
                                        #"earliest dropoff time" : "xxx",
                                        #"latest dropoff time" : "xxx",
                                        #"pickup address" : "xxx",
                                        #"pickup unit number" : "xxx",
                                        #"pickup postal code" : "xxx",
                                        #"earliest pickup time" : "xxx",
                                        #"latest pickup time" : "xxx",
                                        #"demand type" : "xxx",
                                        #"demand load" : "xxx",
                                        #"remarks" : "xxx",
                                        #"phone number" : "xxx",
                                        "pickup service time" : 0,
                                        "dropoff service time" : 480})
    generate_template(file)
    return file.df_new
