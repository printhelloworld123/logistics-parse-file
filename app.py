import pandas as pd
from datetime import datetime
import numpy as np
import regex as re

import streamlit as st
import base64
import json
import os
import io

import functions
from functions import *

import templates
from templates import *

def main():
    try:
        sidebar = ["Prepare file from scratch", "Use existing template"]
        choice = st.sidebar.selectbox("Select option", sidebar)

        if choice == "Prepare file from scratch":

            st.title("Prepare file from scratch")


            st.subheader("1.Upload CSV File")
            uploaded_file = st.file_uploader("Upload CSV", type="csv", accept_multiple_files=False)
            df = pd.read_csv(uploaded_file)
            file = File(df)

            
            st.subheader("2. Map columns")
            file.df_new

            option_cols_service_time = [col for col in list(file.df_new.columns)] + ['NIL']
            st.write("###")
            st.write("2.1 Select the reference column for service time (Select NIL if this step should be skipped)")
            service_time = st.selectbox("Reference column for service time", options=option_cols_service_time)
            
            file.duplicate_service_times(service_time)

            st.write("###")
            st.write("2.2 Select the columns from the file, corresponding to each of the stated column")

            r1c1, r1c2, r1c3 = st.beta_columns([2,2,2])
            r2c1, r2c2, r2c3 = st.beta_columns([2,2,2])
            r3c1, r3c2, r3c3 = st.beta_columns([2,2,2])
            r4c1, r4c2, r4c3 = st.beta_columns([2,2,2])
            r5c1, r5c2, r5c3 = st.beta_columns([2,2,2])
            r6c1, r6c2, r6c3 = st.beta_columns([2,2,2])

            option_cols = [col for col in list(file.df_new.columns)] + ['Not in file']

            id = r1c1.selectbox("id", options=option_cols, index=len(option_cols)-1)
            name = r1c2.selectbox("name", options=option_cols,index=len(option_cols)-1)
            dropoff_add = r1c3.selectbox("dropoff address", options=option_cols,index=len(option_cols)-1)
            dropoff_unit_num = r2c1.selectbox("dropoff unit number", options=option_cols,index=len(option_cols)-1)
            dropoff_postal_code = r2c2.selectbox("dropoff postal code", options=option_cols,index=len(option_cols)-1)
            earliest_dropoff_time = r2c3.selectbox("earliest dropoff time", options=option_cols,index=len(option_cols)-1)
            latest_dropoff_time = r3c1.selectbox("latest dropoff time", options=option_cols,index=len(option_cols)-1)
            pickup_address = r3c2.selectbox("pickup address", options=option_cols,index=len(option_cols)-1)
            pickup_unit_num = r3c3.selectbox("pickup unit number", options=option_cols,index=len(option_cols)-1)
            pickup_postal_code = r4c1.selectbox("pickup postal code", options=option_cols,index=len(option_cols)-1)
            earliest_pickup_time = r4c2.selectbox("earliest pickup time", options=option_cols,index=len(option_cols)-1)
            latest_pickup_time = r4c3.selectbox("latest pickup time", options=option_cols,index=len(option_cols)-1)
            demand_type = r5c1.selectbox("demand type", options=option_cols,index=len(option_cols)-1)
            demand_load = r5c2.selectbox("demand load", options=option_cols,index=len(option_cols)-1)
            remarks = r5c3.selectbox("remarks", options=option_cols,index=len(option_cols)-1)
            phone_num = r6c1.selectbox("phone number ", options=option_cols,index=len(option_cols)-1)
            pickup_service_time = r6c2.selectbox("pickup service time", options=option_cols,index=len(option_cols)-1)
            dropoff_service_time = r6c3.selectbox("dropoff service time", options=option_cols,index=len(option_cols)-1)
            
            selected_cols = {   id: 'id',
                                name: 'name',
                                dropoff_add: 'dropoff address',
                                dropoff_unit_num: 'dropoff unit number',
                                dropoff_postal_code: 'dropoff postal code',
                                earliest_dropoff_time: 'earliest dropoff time',
                                latest_dropoff_time: 'latest dropoff time',
                                pickup_address: 'pickup address',
                                pickup_unit_num: 'pickup unit number',
                                pickup_postal_code: 'pickup postal code',
                                earliest_pickup_time: 'earliest pickup time',
                                latest_pickup_time: 'latest pickup time',
                                demand_type: 'demand type',
                                demand_load: 'demand load',
                                remarks: 'remarks',
                                phone_num: 'phone number',
                                pickup_service_time: 'pickup service time',
                                dropoff_service_time: 'dropoff service time' }
            
            cols_to_rename = {key:value for key, value in list(filter(lambda x: 
                                                                        x[0] != "Not in file", list(selected_cols.items())))}
            

            file.rename_columns_general(cols_to_rename)
            file.remove_nil_rows()


            st.subheader("3. Input values for selected columns")
            file.df_new
            
            st.write("###")
            st.write("3.1 Configure the values for each column if necessary")

            r1c1_val, r1c2_val, r1c3_val = st.beta_columns([2,2,2])
            r2c1_val, r2c2_val, r2c3_val = st.beta_columns([2,2,2])
            r3c1_val, r3c2_val, r3c3_val = st.beta_columns([2,2,2])
            r4c1_val, r4c2_val, r4c3_val = st.beta_columns([2,2,2])
            r5c1_val, r5c2_val, r5c3_val = st.beta_columns([2,2,2])
            r6c1_val, r6c2_val, r6c3_val = st.beta_columns([2,2,2])

            id_val = r1c1_val.text_input("id")
            name_val = r1c2_val.text_input("name")
            dropoff_add_val = r1c3_val.text_input("dropoff address")
            dropoff_unit_num_val = r2c1_val.text_input("dropoff unit number")
            dropoff_postal_code_val = r2c2_val.text_input("dropoff postal code")
            earliest_dropoff_time_val = r2c3_val.text_input("earliest dropoff time")
            latest_dropoff_time_val = r3c1_val.text_input("latest dropoff time")
            pickup_address_val = r3c2_val.text_input("pickup address")
            pickup_unit_num_val = r3c3_val.text_input("pickup unit number")
            pickup_postal_code_val = r4c1_val.text_input("pickup postal code")
            earliest_pickup_time_val = r4c2_val.text_input("earliest pickup time")
            latest_pickup_time_val = r4c3_val.text_input("latest pickup time")
            demand_type_val = r5c1_val.text_input("demand type")
            demand_load_val = r5c2_val.text_input("demand load")
            remarks_val = r5c3_val.text_input("remarks")
            phone_num_val = r6c1_val.text_input("phone number")
            pickup_service_time_val = r6c2_val.text_input("pickup service time")
            dropoff_service_time_val = r6c3_val.text_input("dropoff service time")
            
            selected_cols_val = {   'id':id_val,
                                    'name': name_val,
                                    'dropoff address': dropoff_add_val,
                                    'dropoff unit number':dropoff_unit_num_val,
                                    'dropoff postal code': dropoff_postal_code_val,
                                    'earliest dropoff time': earliest_dropoff_time_val,
                                    'latest dropoff time':latest_dropoff_time_val,
                                    'pickup address':pickup_address_val,
                                    'pickup unit number':pickup_unit_num_val,
                                    'pickup postal code':pickup_postal_code_val,
                                    'earliest pickup time':earliest_pickup_time_val,
                                    'latest pickup time':latest_pickup_time_val,
                                    'demand type':demand_type_val,
                                    'demand load':demand_load_val,
                                    'remarks':remarks_val,
                                    'phone number':phone_num_val,
                                    'pickup service time':pickup_service_time_val,
                                    'dropoff service time':dropoff_service_time_val     }
            
            cols_to_input_val = {key:value for key, value in list(filter(lambda x: 
                                                                        len(x[1]) != 0, list(selected_cols_val.items())))}
            
            file.set_row_values_general(cols_to_input_val)
            file.extract_unit_num_pickup()
            file.extract_unit_num_dropoff()
            file.extract_postal_code_from_pickup_address()
            file.extract_postal_code_from_dropoff_address()
            file.normalize_pickup_postal_code()
            file.normalize_dropoff_postal_code()
            file.convert_earliest_dropoff_time()
            file.extract_earliest_dropoff_time()
            file.extract_latest_dropoff_time()
            file.extract_earliest_pickup_time()
            file.extract_latest_pickup_time()
            file.convert_earliest_dropoff_time()
            file.convert_latest_dropoff_time()
            file.convert_earliest_pickup_time()
            file.convert_latest_pickup_time()
            file.convert_phone_nos_all()
            

            st.subheader("4. Download file")
            file.df_new
            csv = file.df_new.to_csv(index=False)
            b64 = base64.b64encode(csv.encode()).decode()  
            csv_filename = str(uploaded_file.name).split('.csv')[0].replace(" ","") + '_processed.csv'
            link1= f'<a href="data:file/csv;base64,{b64}" download={csv_filename}>Download csv file</a>'
            st.markdown(link1, unsafe_allow_html=True)

            towrite = io.BytesIO()
            downloaded_file = file.df_new.to_excel(towrite, encoding='utf-8', index=False, header=True)
            towrite.seek(0)
            b64 = base64.b64encode(towrite.read()).decode() 
            excel_filename = str(uploaded_file.name).split('.csv')[0].replace(" ","") + '_processed.xlsx'
            link2= f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download={excel_filename}>Download excel file</a>'
            st.markdown(link2, unsafe_allow_html=True)
        
        elif choice == "Use existing template":
            st.title("Use existing template")


            st.subheader("1.Upload CSV File")
            uploaded_file = st.file_uploader("Upload CSV", type="csv", accept_multiple_files=False)
            df = pd.read_csv(uploaded_file)

            st.subheader("2.Choose template")
            df
            templates = ["Assurity", "Interconntinental Hotel", "Shangri-La Hotel (Mooncakes)", 
                        "Herbalife (Mooncakes)", "Wholefish"]
            selected_template = st.selectbox("Template", options=templates)
            if selected_template == "Assurity":
                prepared_file = prepare_file_assurity(df)
            elif selected_template == "Interconntinental Hotel":
                prepared_file = prepare_file_interconntinental_hotel(df)
            elif selected_template == "Shangri-La Hotel (Mooncakes)":
                prepared_file = prepare_file_shangrila_hotel(df)
            elif selected_template == "Herbalife (Mooncakes)":
                prepared_file = prepare_file_herbalife(df)
            elif selected_template == "Wholefish":
                prepared_file = prepare_file_wholefish(df)
            

            st.subheader("3. Download file")

            prepared_file
            csv = prepared_file.to_csv(index=False)
            b64 = base64.b64encode(csv.encode()).decode()  
            csv_filename = str(uploaded_file.name).split('.csv')[0].replace(" ","") + '_processed.csv'
            link1= f'<a href="data:file/csv;base64,{b64}" download={csv_filename}>Download csv file</a>'
            st.markdown(link1, unsafe_allow_html=True)

            towrite = io.BytesIO()
            downloaded_file = prepared_file.to_excel(towrite, encoding='utf-8', index=False, header=True)
            towrite.seek(0)
            b64 = base64.b64encode(towrite.read()).decode() 
            excel_filename = str(uploaded_file.name).split('.csv')[0].replace(" ","") + '_processed.xlsx'
            link2= f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download={excel_filename}>Download excel file</a>'
            st.markdown(link2, unsafe_allow_html=True)
    except:
        pass
        

if __name__ == "__main__":
    main()