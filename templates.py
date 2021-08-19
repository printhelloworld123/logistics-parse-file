import pandas as pd
from datetime import datetime
import numpy as np
import regex as re

import functions
from functions import *

def prepare_file_assurity(df):
    assurity_file = File(df)
    #xxx.duplicate_service_times("xxx")
    assurity_file.rename_columns_general({
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
    assurity_file.remove_nil_rows()
    assurity_file.set_row_values_general({
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

    assurity_file.extract_unit_num_pickup()
    assurity_file.extract_unit_num_dropoff()
    assurity_file.extract_postal_code_from_pickup_address()
    assurity_file.extract_postal_code_from_dropoff_address()
    assurity_file.normalize_pickup_postal_code()
    assurity_file.normalize_dropoff_postal_code()
    assurity_file.convert_earliest_dropoff_time()
    assurity_file.convert_latest_dropoff_time()
    assurity_file.convert_earliest_pickup_time()
    assurity_file.convert_latest_pickup_time()
    assurity_file.convert_phone_nos_all()
    return assurity_file.df_new

def prepare_file_interconntinental_hotel(df):
    interconntinental_hotel_file = File(df)
    #xxx.duplicate_service_times("xxx")
    interconntinental_hotel_file.rename_columns_general({
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
    interconntinental_hotel_file.remove_nil_rows()
    interconntinental_hotel_file.set_row_values_general({
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
    interconntinental_hotel_file.extract_unit_num_pickup()
    interconntinental_hotel_file.extract_unit_num_dropoff()
    interconntinental_hotel_file.extract_postal_code_from_pickup_address()
    interconntinental_hotel_file.extract_postal_code_from_dropoff_address()
    interconntinental_hotel_file.normalize_pickup_postal_code()
    interconntinental_hotel_file.normalize_dropoff_postal_code()
    interconntinental_hotel_file.convert_earliest_dropoff_time()
    interconntinental_hotel_file.convert_latest_dropoff_time()
    interconntinental_hotel_file.convert_earliest_pickup_time()
    interconntinental_hotel_file.convert_latest_pickup_time()
    interconntinental_hotel_file.convert_phone_nos_all()
    return interconntinental_hotel_file.df_new

def prepare_file_shangrila_hotel(df):
    shangrila_hotel_file = File(df)
    shangrila_hotel_file.duplicate_service_times("Delivery Timings")
    shangrila_hotel_file.rename_columns_general({
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
    shangrila_hotel_file.remove_nil_rows()
    
    shangrila_hotel_file.set_row_values_general({
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
    shangrila_hotel_file.extract_unit_num_pickup()
    shangrila_hotel_file.extract_unit_num_dropoff()
    shangrila_hotel_file.extract_postal_code_from_pickup_address()
    shangrila_hotel_file.extract_postal_code_from_dropoff_address()
    shangrila_hotel_file.normalize_pickup_postal_code()
    shangrila_hotel_file.normalize_dropoff_postal_code()
    shangrila_hotel_file.extract_earliest_dropoff_time()
    shangrila_hotel_file.extract_latest_dropoff_time()
    shangrila_hotel_file.extract_earliest_pickup_time()
    shangrila_hotel_file.extract_latest_pickup_time()
    shangrila_hotel_file.convert_earliest_dropoff_time()
    shangrila_hotel_file.convert_latest_dropoff_time()
    shangrila_hotel_file.convert_earliest_pickup_time()
    shangrila_hotel_file.convert_latest_pickup_time()
    shangrila_hotel_file.convert_phone_nos_all()
    return shangrila_hotel_file.df_new

def prepare_file_herbalife(df):
    herbalife = File(df)
    #xxx.duplicate_service_times("xxx")
    herbalife.rename_columns_general({
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
    herbalife.remove_nil_rows()
    herbalife.set_row_values_general({
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
    herbalife.extract_unit_num_pickup()
    herbalife.extract_unit_num_dropoff()
    herbalife.extract_postal_code_from_pickup_address()
    herbalife.extract_postal_code_from_dropoff_address()
    herbalife.normalize_pickup_postal_code()
    herbalife.normalize_dropoff_postal_code()
    herbalife.convert_earliest_dropoff_time()
    herbalife.convert_latest_dropoff_time()
    herbalife.convert_earliest_pickup_time()
    herbalife.convert_latest_pickup_time()
    herbalife.convert_phone_nos_all()
    return herbalife.df_new

def prepare_file_wholefish(df):
    wholefish = File(df)
    #xxx.duplicate_service_times("xxx")
    wholefish.rename_columns_general({
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
    wholefish.remove_nil_rows()
    wholefish.set_row_values_general({
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
    wholefish.extract_unit_num_pickup()
    wholefish.extract_unit_num_dropoff()
    wholefish.extract_postal_code_from_pickup_address()
    wholefish.extract_postal_code_from_dropoff_address()
    wholefish.normalize_pickup_postal_code()
    wholefish.normalize_dropoff_postal_code()
    wholefish.convert_earliest_dropoff_time()
    wholefish.convert_latest_dropoff_time()
    wholefish.convert_earliest_pickup_time()
    wholefish.convert_latest_pickup_time()
    wholefish.convert_phone_nos_all()
    return wholefish.df_new
