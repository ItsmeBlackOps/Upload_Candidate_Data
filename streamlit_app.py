import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
from random import randint
import os

# Initialize Firebase app
if not firebase_admin._apps:
    cred = credentials.Certificate('serviceKey.json')
    firebase_admin.initialize_app(cred)

# Firestore client
db = firestore.client()

# Required columns for the data
REQUIRED_COLUMNS = [
    'Candidate', "Candidate's email address", 'Location', 'Manager',
    'Marketing start date', 'Open to Relocate', 'Phone number', 'Random URL',
    'Recruiter', 'Status', 'Team Lead', 'Technology', 'Upfront', 'Visa', 'Branch'
]

def validate_and_format_data(df):
    # Check for missing columns
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
    
    # Generate random URLs for 'Random URL' column if not already present
    if 'Random URL' not in df.columns:
        urls = [
            f'https://firebasestorage.googleapis.com/v0/b/reportcraft-164f6.appspot.com/o/avatar_{randint(1, 25)}.jpg?alt=media&token=06d72c5c-cc8a-41ff-bf1f-1d12ccc7208f'
            for _ in range(len(df))
        ]
        df['Random URL'] = urls
    
    return df[REQUIRED_COLUMNS]

def update_collection_if_not_exists(collection_name, csv_file_path):
    try:
        # Load CSV file into DataFrame
        df = pd.read_csv(csv_file_path, encoding='UTF-8')

        # Validate and format data
        df = validate_and_format_data(df)

        # Debug: Print DataFrame columns
        st.write("DataFrame columns:", df.columns)

        # Convert the 'Candidate' column to a set for fast lookup
        candidate_set = set(df['Candidate'])

        # Reference to the collection
        collection_ref = db.collection(collection_name)

        # Get all documents in the collection
        docs = collection_ref.stream()

        # Convert Firestore documents to a set of candidates
        existing_candidates = {doc.to_dict().get('Candidate') for doc in docs}

        # Debug: Print existing candidates
        st.write("Existing candidates in Firestore:", existing_candidates)

        # Loop through each row in the DataFrame
        for index, row in df.iterrows():
            candidate = row['Candidate']

            # Check if candidate exists in Firestore collection
            if candidate in existing_candidates:
                st.write(f"Skipping existing candidate: {candidate}")
            else:
                # Add new candidate to the collection
                collection_ref.add(row.to_dict())
                st.write(f"Added new candidate: {candidate}")

    except Exception as e:
        st.error(f"Error updating collection: {e}")

# Streamlit app interface
st.title('Firestore Collection Updater')

# Upload CSV file
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    try:
        # Save uploaded file to a temporary location
        temp_file_path = os.path.join("/tmp", uploaded_file.name)
        with open(temp_file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())

        st.write(f"Uploaded file: {uploaded_file.name}")

        # Load CSV file into DataFrame to validate columns
        df = pd.read_csv(temp_file_path, encoding='UTF-8')
        validate_and_format_data(df)  # This will raise an error if columns are not correct

        # If validation passes, proceed with updating Firestore
        collection_name = 'Candidates'
        update_collection_if_not_exists(collection_name, temp_file_path)

    except Exception as e:
        st.error(f"Error: {e}")
