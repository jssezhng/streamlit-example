from collections import namedtuple
import altair as alt
import math
import pandas as pd
import streamlit as st
import json
import requests


DATA_WAREHOUSE_API_URL = st.secrets['data_warehouse_api_url']
DATA_WAREHOUSE_API_KEY = st.secrets['data_warehouse_api_key']
BATCH_SIZE = 10

"""
# Skiptracing Tool

Input your file below that you would like skiptraced. Required headers are "ADDRESS STREET", "ADDRESS CITY", "ADDRESS ZIP". Optional headers are "LLC NAME", "FIRST NAME", "LAST NAME".
"""

uploaded_file = st.file_uploader("Upload File(s)", type=None, key=None, help=None, on_change=None, args=None, kwargs=None, disabled=False, label_visibility="visible")
if uploaded_file is not None:
    # Can be used wherever a "file-like" object is accepted:
    cols = ["ADDRESS STREET", "ADDRESS CITY", "ADDRESS ZIP", "LLC NAME", "FIRST NAME", "LAST NAME"]
    df = pd.read_csv(uploaded_file, usecols=lambda c: c in set(cols), keep_default_na=False, dtype=object)
    st.write(df)

    def chunker(seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    output_list = []
    progress_bar = st.progress(0, text="SKIPTRACING IN PROGRESS")
    for batch in chunker(df, BATCH_SIZE):
        inputs = []
        try: 
            for row in batch.index:
                address_street = batch['ADDRESS STREET'][row]
                address_city = batch['ADDRESS CITY'][row]
                address_zip = batch['ADDRESS ZIP'][row]
                llc_name = batch['LLC NAME'][row] if 'LLC NAME' in batch.columns else ''
                first_name = batch['FIRST NAME'][row] if 'FIRST NAME' in batch.columns else ''
                last_name = batch['LAST NAME'][row] if 'LAST NAME' in batch.columns else ''
                inputs.append({
                    "address_street": address_street,
                    "address_city": address_city,
                    "address_zip": address_zip,
                    "llc_name": llc_name,
                    "first_name": first_name,
                    "last_name": last_name,
                })

            headers = {
                "X-API-KEY": DATA_WAREHOUSE_API_KEY,
                "content-type": "application/json",
                "accept": "application/json",
            }

            json_body = json.dumps({"inputs": inputs})
            batch_response = requests.request(
                "POST", DATA_WAREHOUSE_API_URL, data=json_body, headers=headers
            )

            if batch_response.status_code < 200 or batch_response.status_code > 299:
                st.error('Error skiptracing from API', icon="🚨")

            for i, res in enumerate(batch_response.json().get('data', [])):
                if res is not None:
                    output_list.append({
                        'ADDRESS STREET': inputs[i].get('address_street', ''),
                        'ADDRESS CITY': inputs[i].get('address_city', ''),
                        'ADDRESS ZIP': inputs[i].get('address_zip', ''),
                        'LLC NAME': inputs[i].get('llc_name', ''),
                        'FIRST NAME': inputs[i].get('first_name', ''),
                        'LAST NAME': inputs[i].get('last_name', ''),
                        'AGE': res.get('age'),
                        'PHONE': res.get('phone'),
                        'PRIMARY NAME': res.get('primary_name'),
                        'PRIMARY EMAIL': res.get('primary_email'),
                        'ALL PHONES': res.get('all_phones'),
                        'ALL NAMES': res.get('all_names'),
                        'ALL EMAILS': res.get('all_emails'),
                    })
                else:
                    output_list.append({
                        'ADDRESS STREET': inputs[i].get('address_street', ''),
                        'ADDRESS CITY': inputs[i].get('address_city', ''),
                        'ADDRESS ZIP': inputs[i].get('address_zip', ''),
                        'LLC NAME': inputs[i].get('llc_name', ''),
                        'FIRST NAME': inputs[i].get('first_name', ''),
                        'LAST NAME': inputs[i].get('last_name', '')
                    })
        except:
            st.error('Error skiptracing. Please ping Jesse for assistance.', icon="🚨")
        progress_bar.progress(len(output_list) / len(df))

    # columns=['ADDRESS STREET', 'ADDRESS CITY', 'ADDRESS ZIP', 'LLC NAME' 'FIRST NAME', 'LAST NAME', 'AGE', 'PHONE', 'PRIMARY NAME', 'PRIMARY EMAIL', 'ALL PHONES', 'ALL NAMES', 'ALL EMAILS']
    output_df = pd.DataFrame(output_list)
    st.write(output_df)

    @st.cache
    def convert_df(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv().encode('utf-8')

    csv = convert_df(output_df)

    st.download_button(
        label="Download output data as CSV",
        data=csv,
        file_name='skiptraced.csv',
        mime='text/csv',
    )
