import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import regex as re

# Function to standardize the pickup and dropoff time columns to a fixed format accepted by RG #
def convert_time(time):
    new_time = str(time)
    new_time = new_time.replace(" ","")
    new_time = new_time.lower()
    new_time = new_time.replace(":","")
    try:
        if "am" in new_time or "pm" in new_time:
            if len(new_time) < 5:
                in_time = datetime.strptime(new_time, "%I%p")
                out_time = datetime.strftime(in_time, "%H:%M")
                return out_time
            elif len(new_time) >= 5:
                in_time = datetime.strptime(new_time, "%I%M%p")
                out_time = datetime.strftime(in_time, "%H:%M")
                return out_time
        elif "am" not in new_time and "pm" not in new_time:
            if len(new_time) < 3:
                first_digits = int(new_time)
                if first_digits <= 12:
                    in_time = datetime.strptime(new_time, "%I")
                    out_time = datetime.strftime(in_time, "%H:%M")
                    return out_time
                elif first_digits > 12:
                    out_time = f"{new_time}:00"
                    return out_time
            elif len(new_time) >= 3:
                first_digits = int(new_time[:-2])
                if first_digits <= 12:
                    in_time = datetime.strptime(new_time, "%I%M")
                    out_time = datetime.strftime(in_time, "%H:%M")
                    return out_time
                elif first_digits > 12:
                    out_time = f"{first_digits}:00"
                    return out_time
    except:
        return time

# Function to create copies of the service time data #
def duplicate_service_times(service_time_col_name):
    try:
        if service_time_col_name != "NIL":
            return service_time_col_name
        else:
            return ""
    except:
        return service_time_col_name

# Function to extract the dropoff time from a given time range #
def extract_dropoff_time(time):
    try:
        if "to" in time:
            time_string = time.split("to")
            dropoff_time = time_string[-1]
            return dropoff_time
        elif "-" in time:
            time_string = time.split("-")
            dropoff_time = time_string[-1]
            return dropoff_time
        else:
            dropoff_time = time
            return dropoff_time
    except:
        return time


# Function to extract the pickup time from a given time range #
def extract_pickup_time(time):
    try:
        if "to" in time:
            time_string = time.split("to")
            pickup_time = time_string[0]
            return pickup_time
        elif "-" in time:
            time_string = time.split("-")
            pickup_time = time_string[0]
            return pickup_time
        else:
            pickup_time = time
            return pickup_time
    except:
        return time

# Function to extract the unit number from a given address string #
def extract_unit_num(address):
    try:
        unit_num = address.replace(" ", ",").split(",")
        if "#" in address:
            unit_num = list(filter(lambda x: "#" in x, unit_num))
            if len(unit_num) == 1:
                unit_num = unit_num[0]
                return unit_num
            elif len(unit_num) > 1:
                unit_num = f"More than 1 unit number identified: {unit_num}"
                return unit_num
        elif "#" not in address and '-' in address:
            unit_num = list(filter(lambda x: "-" in x, unit_num))
            if len(unit_num) == 1:
                unit_num = unit_num[0]
                return unit_num
            elif len(unit_num) > 1:
                unit_num = f"More than 1 unit number identified: {unit_num}"
                return unit_num
    except:
        return address

# Function to extract the postal code from a given address string #
def extract_postal_code_from_address(address, postal_code):
    try:
        code = re.findall(r'(\d{6})', address)
        if len(set(code)) > 1:
            final_code = address
            return final_code
        else:
            final_code = code[0]
            return final_code
    except:
        return postal_code

# Function to standardize the postal code to a fixed format accepted by RG (ie 6 digit string) #
def normalize_postal_code(code):
    try:
        new_code = str(int(float(code)))
        if len(new_code) == 5:
            new_code = "0" + new_code
            return new_code
        else:
            return new_code
    except:
        new_code = re.sub(r'[^\w\s]', "", code)
        return new_code

# Function to standardize the phone number to a fixed format accepted by RG (+65 followed by 8 digits)
def standardize_phone_nos(nos):
    try:
        if str(nos) == "nan":
            new_nos = nos
            return new_nos
        elif str(nos) != "nan":
            new_nos = str(nos)
            new_nos = new_nos.split(".")[0]
            new_nos = new_nos.replace(" ", "")
            if "+65" not in new_nos:
                if new_nos[0:2] == "65" and len(new_nos) == 10:
                    new_nos = f"+{new_nos}"
                    return new_nos
                elif new_nos[0:2] != "65" and len(new_nos) == 8:
                    new_nos = f"+65{new_nos}"
                    return new_nos
            elif "+65" in new_nos:
                if new_nos[0:3] == "+65" and len(new_nos) ==11:
                    return new_nos
            return f"Phone number provided may be formatted wrongly: Please check: {new_nos}"
    except:
        return nos

class File:
    def __init__(self, df):
        self.df = df
        self.df_new = self.df.copy()
        
    # Method to rename the columns in client's file to column names accepted by RG
    def rename_columns_general(self, d_columns_to_rename):
        try:
            self.df_new = self.df_new.rename(columns=d_columns_to_rename)
            req_columns=['id','name','dropoff address','dropoff unit number',
                        'dropoff postal code','earliest dropoff time',
                        'latest dropoff time','pickup address','pickup unit number','pickup postal code',
                        'earliest pickup time','latest pickup time','demand type','demand load','remarks',
                        'phone number','pickup service time','dropoff service time']
            self.df_new = self.df_new.reindex(columns=req_columns)
        except Exception as e:
            print(f"rename_columns_general:{e}")
        return self.df_new
    
    # Method to assign a fixed value for all rows in a specified column #
    def set_row_values_general(self, d_row_values):
        try:
            for key, value in list(d_row_values.items()):
                self.df_new[key] = value
        except Exception as e:
            print(f"set_row_values_general:{e}")
        return self.df_new
    
    # Method to remove rows with no data input #
    def remove_nil_rows(self):
        try:
            self.df_new = self.df_new.dropna(subset=["name"])
        except Exception as e:
            print(f"remove_nil_rows:{e}")
        return self.df_new
    
    # Method to extract the unit number from the pickup address #
    def extract_unit_num_pickup(self):
        try:
            self.df_new["pickup unit number"] = list(map(lambda x: extract_unit_num(x), 
                                                    list(self.df_new["pickup address"])))
        except Exception as e:
            print(f"extract_unit_num_pickup:{e}")
        return self.df_new

    # Method to extract the unit number from the dropoff address #
    def extract_unit_num_dropoff(self):
        try:
            self.df_new["dropoff unit number"] = list(map(lambda x: extract_unit_num(x), 
                                                    list(self.df_new["dropoff address"])))
        except Exception as e:
            print(f"extract_unit_num_dropoff:{e}")
        return self.df_new
                                    
    # Method to extract the postal code from the pickup address #
    def extract_postal_code_from_pickup_address(self):
        try:
            self.df_new["pickup postal code"] = list(map(lambda x,y: extract_postal_code_from_address(x,y),
                                                    list(self.df_new["pickup address"]),
                                                    list(self.df_new["pickup postal code"])))
        except Exception as e:
            print(f"extract_postal_code_from_pickup_address:{e}")
        return self.df_new
    
    # Method to extract the postal code from the dropoff address #
    def extract_postal_code_from_dropoff_address(self):
        try:
            self.df_new["dropoff postal code"] = list(map(lambda x, y:extract_postal_code_from_address(x, y), 
                                                     list(self.df_new["dropoff address"]),
                                                     list(self.df_new["dropoff postal code"])))
        except Exception as e:
            print(f"extract_postal_code_from_dropoff_address:{e}")
        return self.df_new
    
    # Method to standardize the pickup postal code to a format accepted by RG #
    def normalize_pickup_postal_code(self):
        try:
            self.df_new["pickup postal code"] = list(map(lambda x: normalize_postal_code(x), 
                                                         list(self.df_new["pickup postal code"])))
        except Exception as e:
            print(f"normalize_pickup_postal_code:{e}")                                          
        return self.df_new
    
    # Method to standardize the dropoff postal code to a format accepted by RG #
    def normalize_dropoff_postal_code(self):
        try:
            self.df_new["dropoff postal code"] = list(map(lambda x: normalize_postal_code(x), 
                                                         list(self.df_new["dropoff postal code"])))
        except Exception as e:
            print(f"normalize_dropoff_postal_code:{e}")                                        
        return self.df_new
    
    # Method to create new columns (earliest dropoff time, latest dropoff time, 
    # earliest pickup time, latest pickup time) with data duplicated from the service time range column # 
    def duplicate_service_times_all(self, service_time_col_name):
        try:
            self.df_new["earliest dropoff time"] = duplicate_service_times(service_time_col_name)
            self.df_new["latest dropoff time"] = duplicate_service_times(service_time_col_name)
            self.df_new["earliest pickup time"] = duplicate_service_times(service_time_col_name)
            self.df_new["latest pickup time"] = duplicate_service_times(service_time_col_name)
        except Exception as e:
            print(f"duplicate_service_times_all:{e}") 
        return self.df_new
    
    # Method to standardize the earliest dropoff time to a format accepted by RG #
    def convert_earliest_dropoff_time(self):
        try:
            self.df_new["earliest dropoff time"] = list(map(lambda x: convert_time(x), 
                                                            list(self.df_new["earliest dropoff time"])))
        except Exception as e:
            print(f"convert_earliest_dropoff_time:{e}")
        return self.df_new
    
    # Method to standardize the latest dropoff time to a format accepted by RG #
    def convert_latest_dropoff_time(self):
        try:
            self.df_new["latest dropoff time"] = list(map(lambda x: convert_time(x), 
                                                            list(self.df_new["latest dropoff time"])))
        except Exception as e:
            print(f"convert_latest_dropoff_time:{e}")
        return self.df_new
    
    # Method to standardize the earliest pickup time to a format accepted by RG #
    def convert_earliest_pickup_time(self):
        try:
            self.df_new["earliest pickup time"] = list(map(lambda x: convert_time(x),
                                                           list(self.df_new["earliest pickup time"])))
        except Exception as e:
            print(f"convert_earliest_pickup_time:{e}")
        return self.df_new
    
    # Method to standardize the latest pickup time to a format accepted by RG #
    def convert_latest_pickup_time(self):
        try:
            self.df_new["latest pickup time"] = list(map(lambda x: convert_time(x),
                                                           list(self.df_new["latest pickup time"])))
        except Exception as e:
            print(f"convert_latest_pickup_time:{e}")
        return self.df_new
    
    # Method to extract the earliest dropoff time from a given time range #
    def extract_earliest_dropoff_time(self):
        try:
            self.df_new["earliest dropoff time"] = list(map(lambda x: extract_dropoff_time(x),
                                                            list(self.df_new["earliest dropoff time"])))
        except Exception as e:
            print(f"extract_earliest_dropoff_time:{e}")
        return self.df_new
    
    # Method to extract the latest dropoff time from a given time range #
    def extract_latest_dropoff_time(self):
        try:
            self.df_new["latest dropoff time"] = list(map(lambda x: extract_dropoff_time(x),
                                                            list(self.df_new["latest dropoff time"])))
        except Exception as e:
            print(f"extract_latest_dropoff_time:{e}")
        return self.df_new

    # Method to extract the earliest pickup time from a given time range #
    def extract_earliest_pickup_time(self):
        try:
            self.df_new["earliest pickup time"] = list(map(lambda x: extract_pickup_time(x),
                                                            list(self.df_new["earliest pickup time"])))
        except Exception as e:
            print(f"extract_earliest_pickup_time:{e}")
        return self.df_new
    
    # Method to extract the latest pickup time from a given time range #
    def extract_latest_pickup_time(self):
        try:
            self.df_new["latest pickup time"] = list(map(lambda x: extract_pickup_time(x),
                                                            list(self.df_new["latest pickup time"])))
        except Exception as e:
            print(f"extract_latest_pickup_time:{e}")
        return self.df_new

    # Method to standardize the phone numbers to a formated accepted by RG#
    def convert_phone_nos_all(self):
        try:
            self.df_new["phone number"] = list(map(lambda x: standardize_phone_nos(x),
                                                           list(self.df_new["phone number"])))
        except Exception as e:
            print(f"convert_phone_nos_all:{e}")
        return self.df_new
