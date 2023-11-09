from collections import namedtuple
import altair as alt
import math
import pandas as pd
import streamlit as st
import json
import requests
from difflib import ndiff


DATA_WAREHOUSE_API_URL = st.secrets['data_warehouse_api_url']
# for local dev
# DATA_WAREHOUSE_API_URL = 'http://localhost:8010'
DATA_WAREHOUSE_API_KEY = st.secrets['data_warehouse_api_key']
BATCH_SIZE = 10

def levenshtein_distance(str1, str2):
    counter = {"+": 0, "-": 0}
    distance = 0
    for edit_code, *_ in ndiff(str1, str2):
        if edit_code == " ":
            distance += max(counter.values())
            counter = {"+": 0, "-": 0}
        else: 
            counter[edit_code] += 1
    distance += max(counter.values())
    return distance

# displays 20101201 as 2010-12-01
def display_dates(date: str):
    if not date:
        return ''
    return date[:4] + '-' + date[4:6] + '-' + date[6:]

tab1, tab2 = st.tabs(["Individual Skiptrace", "Bulk Skiptrace"])
with tab1:
    """
    # Skiptracing Tool
    """
    first_name = st.text_input('First Name')
    last_name = st.text_input('Last Name')
    address_street = st.text_input('Address Street')
    address_city = st.text_input('Address City')
    address_state = st.text_input('Address State')
    address_zip = st.text_input('Address Zip')
    phone = st.text_input('Phone')
    email = st.text_input('Email')
    # Disable the submit button after it is clicked
    def disable():
        st.session_state.disabled = True

    # Initialize disabled for form_submit_button to False
    if "disabled" not in st.session_state:
        st.session_state.disabled = False

    submit = st.button("Submit", key='individual_skiptrace', on_click=disable, disabled=st.session_state.disabled)
    if submit:
        input_params = {
            "address_street": address_street,
            "address_city": address_city,
            "address_zip": address_zip,
            "address_state": address_state,
            "first_name": first_name,
            "last_name": last_name,
            "phone": phone,
            "email": email,
        }

        headers = {
            "X-API-KEY": DATA_WAREHOUSE_API_KEY,
            "content-type": "application/json",
            "accept": "application/json",
        }

        json_body = json.dumps(input_params)
        batch_response = requests.request(
            "POST", DATA_WAREHOUSE_API_URL + "/skiptrace", data=json_body, headers=headers
        )

        if batch_response.status_code < 200 or batch_response.status_code > 299:
            st.error('Error skiptracing from API', icon="🚨")
        
        if len(batch_response.json().get('data', [])) > 0:
            responses = batch_response.json().get('data', [])
            sorted_responses = sorted(responses, key=lambda x: levenshtein_distance(first_name + ' ' + last_name, x.get('primary_name', '')) )
            for response in sorted_responses:
                st.title('Name: ' + response.get('primary_name'))
                st.title('Phone: ' + response.get('phone'))
                st.title('Email: ' + response.get('primary_email'))
                if len(response.get('all_emails')) > 0: 
                    response['all_emails'] = [{"email": email.get('email'), "first_seen": display_dates(email.get('first_seen')), "last_seen": display_dates(email.get('last_seen'))} for email in response.get('all_emails') if email != response.get('primary_email')]
                if len(response.get('all_phones')) > 0: 
                    response['all_phones'] = [{"phone": phone.get('phone'), "first_seen": display_dates(phone.get('first_seen')), "last_seen": display_dates(phone.get('last_seen'))} for phone in response.get('all_phones') if phone != response.get('phone')]
                if len(response.get('all_addresses')) > 0: 
                    response['all_addresses'] = [{"formatted_address": address.get('formatted_address'), "first_seen": display_dates(address.get('first_seen')), "last_seen": display_dates(address.get('last_seen'))} for address in response.get('all_addresses') if address != response.get('address_street')]
                st.write(response)
                st.divider()



with tab2:
    """
    # Bulk Skiptracing Tool

    Input your file below that you would like skiptraced. Headers are "ADDRESS STREET", "ADDRESS CITY", "ADDRESS ZIP", "ADDRESS STATE", "LLC NAME", "FIRST NAME", "LAST NAME", "EMAIL". You must have either contact information or some combination of address information and contact information
    """

    def run_skiptracing_on_df(df):
        def chunker(seq, size):
            return (seq[pos:pos + size] for pos in range(0, len(seq), size))

        output_list = []
        progress_bar = st.progress(0, text="SKIPTRACING IN PROGRESS")
        progress = st.empty()
        for batch in chunker(df, BATCH_SIZE):
            inputs = []
            try: 
                for row in batch.index:
                    address_street = batch['ADDRESS STREET'][row] if 'ADDRESS STREET' in batch.columns else ''
                    address_city = batch['ADDRESS CITY'][row] if 'ADDRESS CITY' in batch.columns else ''
                    address_zip = batch['ADDRESS ZIP'][row] if 'ADDRESS ZIP' in batch.columns else ''
                    address_state = batch['ADDRESS STATE'][row] if 'ADDRESS STATE' in batch.columns else ''
                    llc_name = batch['LLC NAME'][row] if 'LLC NAME' in batch.columns else ''
                    first_name = batch['FIRST NAME'][row] if 'FIRST NAME' in batch.columns else ''
                    last_name = batch['LAST NAME'][row] if 'LAST NAME' in batch.columns else ''
                    email = batch['EMAIL'][row] if 'EMAIL' in batch.columns else ''
                    inputs.append({
                        "address_street": address_street,
                        "address_city": address_city,
                        "address_zip": address_zip,
                        "address_state": address_state,
                        "llc_name": llc_name,
                        "first_name": first_name,
                        "last_name": last_name,
                        "email": email,
                    })

                headers = {
                    "X-API-KEY": DATA_WAREHOUSE_API_KEY,
                    "content-type": "application/json",
                    "accept": "application/json",
                }

                json_body = json.dumps({"inputs": inputs})
                batch_response = requests.request(
                    "POST", DATA_WAREHOUSE_API_URL + "/bulk_skiptrace", data=json_body, headers=headers
                )

                if batch_response.status_code < 200 or batch_response.status_code > 299:
                    st.error('Error skiptracing from API', icon="🚨")

                for i, res in enumerate(batch_response.json().get('data', [])):
                    if res is not None:
                        output = {
                            'ADDRESS STREET': inputs[i].get('address_street', ''),
                            'ADDRESS CITY': inputs[i].get('address_city', ''),
                            'ADDRESS ZIP': inputs[i].get('address_zip', ''),
                            'ADDRESS STATE': inputs[i].get('address_state', ''),
                            'LLC NAME': inputs[i].get('llc_name', ''),
                            'FIRST NAME': inputs[i].get('first_name', ''),
                            'LAST NAME': inputs[i].get('last_name', ''),
                            'AGE': res.get('age'),
                            'PHONE': res.get('phone'),
                            'PRIMARY NAME': res.get('primary_name'),
                            'PRIMARY EMAIL': res.get('primary_email'),
                            'ALL NAMES': res.get('all_names')
                        }
                        output['ALL PHONES'] = [json.dumps({"phone": phone.get('phone'), "first_seen": display_dates(phone.get('first_seen')), "last_seen": display_dates(phone.get('last_seen'))}) for phone in res.get('all_phones', [])]
                        output['ALL EMAILS'] = [json.dumps({"email": email.get('email'), "first_seen": display_dates(email.get('first_seen')), "last_seen": display_dates(email.get('last_seen'))}) for email in res.get('all_emails', [])]
                        output['LINKED PROPERTY COUNT'] = len(res.get('linked_properties', []))
                        linked_property_count = 0
                        for linked_property in res.get('linked_properties', []):
                            linked_property_count += 1
                            output['LINKED PROPERTY ' + str(linked_property_count)+ ' ADDRESS'] = linked_property.get('formatted_address')
                            output['LINK PROPERTY ' + str(linked_property_count) + ' DETAILS'] = linked_property
                        output_list.append(output)
                    else:
                        output_list.append({
                            'ADDRESS STREET': inputs[i].get('address_street', ''),
                            'ADDRESS CITY': inputs[i].get('address_city', ''),
                            'ADDRESS ZIP': inputs[i].get('address_zip', ''),
                            'ADDRESS STATE': inputs[i].get('address_state', ''),
                            'LLC NAME': inputs[i].get('llc_name', ''),
                            'FIRST NAME': inputs[i].get('first_name', ''),
                            'LAST NAME': inputs[i].get('last_name', '')
                        })
            except:
                st.error('Error skiptracing. Please ping Jesse for assistance.', icon="🚨")
            progress_bar.progress(len(output_list) / len(df))
            progress.write("%i/%i records processed" % (len(output_list), len(df)))

        # columns=['ADDRESS STREET', 'ADDRESS CITY', 'ADDRESS ZIP', 'ADDRESS STATE', 'LLC NAME' 'FIRST NAME', 'LAST NAME', 'AGE', 'PHONE', 'PRIMARY NAME', 'PRIMARY EMAIL', 'ALL PHONES', 'ALL NAMES', 'ALL EMAILS']
        output_df = pd.DataFrame(output_list)
        st.title('Output Data')
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

    # Disable the submit button after it is clicked
    def disable():
        st.session_state.disabled = True

    # Initialize disabled for form_submit_button to False
    if "disabled" not in st.session_state:
        st.session_state.disabled = False

    uploaded_file = st.file_uploader("Upload File(s)", type=None, label_visibility="visible")
    if uploaded_file is not None:
        # Can be used wherever a "file-like" object is accepted:
        cols = ["ADDRESS STREET", "ADDRESS CITY", "ADDRESS ZIP", "ADDRESS STATE", "LLC NAME", "FIRST NAME", "LAST NAME", "EMAIL"]
        df = pd.read_csv(uploaded_file, usecols=lambda c: c in set(cols), keep_default_na=False, dtype=object)
        st.title('Input Data')
        st.write(df)
        do_skiptrace = st.button("Run Skiptracing", key='bulk_skiptrace', on_click=disable, disabled=st.session_state.disabled)
        if do_skiptrace:
            run_skiptracing_on_df(df)