#!/usr/bin/env python3
"""
Streamlit App for BigBasket Automation Workflows
Combines Gmail attachment downloader and Excel GRN processor
"""

import streamlit as st
import os
import json
import base64
import tempfile
import time
import logging
import pandas as pd
import zipfile
import re
import io
import warnings
import subprocess
import sys
import math
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from io import StringIO
from lxml import etree

from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow, Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

warnings.filterwarnings("ignore")

# Configure Streamlit page
st.set_page_config(
    page_title="BigBasket Automation",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

class BigBasketAutomation:
    def __init__(self):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None
        
        # API scopes
        self.gmail_scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        self.drive_scopes = ['https://www.googleapis.com/auth/drive']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
    
    def authenticate_from_secrets(self, progress_bar, status_text):
        """Authenticate using Streamlit secrets with web-based OAuth flow"""
        try:
            status_text.text("Authenticating with Google APIs...")
            progress_bar.progress(0.10)
            
            # Check for existing token in session state
            if 'oauth_token' in st.session_state:
                try:
                    combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
                    creds = Credentials.from_authorized_user_info(st.session_state.oauth_token, combined_scopes)
                    if creds and creds.valid:
                        progress_bar.progress(0.50)
                        # Build services
                        self.gmail_service = build('gmail', 'v1', credentials=creds)
                        self.drive_service = build('drive', 'v3', credentials=creds)
                        self.sheets_service = build('sheets', 'v4', credentials=creds)
                        progress_bar.progress(1.00)
                        status_text.text("Authentication successful!")
                        return True
                    elif creds and creds.expired and creds.refresh_token:
                        creds.refresh(Request())
                        st.session_state.oauth_token = json.loads(creds.to_json())
                        # Build services
                        self.gmail_service = build('gmail', 'v1', credentials=creds)
                        self.drive_service = build('drive', 'v3', credentials=creds)
                        self.sheets_service = build('sheets', 'v4', credentials=creds)
                        progress_bar.progress(1.00)
                        status_text.text("Authentication successful!")
                        return True
                except Exception as e:
                    st.info(f"Cached token invalid, requesting new authentication: {str(e)}")
            
            # Use Streamlit secrets for OAuth
            if "google" in st.secrets and "credentials_json" in st.secrets["google"]:
                creds_data = json.loads(st.secrets["google"]["credentials_json"])
                combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
                
                # Configure for web application
                flow = Flow.from_client_config(
                    client_config=creds_data,
                    scopes=combined_scopes,
                    redirect_uri=st.secrets.get("google", {}).get("redirect_uri", "https://bbnet-auto-grn.streamlit.app/")
                )
                
                # Generate authorization URL
                auth_url, _ = flow.authorization_url(prompt='consent')
                
                # Check for callback code
                query_params = st.query_params
                if "code" in query_params:
                    try:
                        code = query_params["code"]
                        flow.fetch_token(code=code)
                        creds = flow.credentials
                        
                        # Save credentials in session state
                        st.session_state.oauth_token = json.loads(creds.to_json())
                        
                        progress_bar.progress(0.50)
                        # Build services
                        self.gmail_service = build('gmail', 'v1', credentials=creds)
                        self.drive_service = build('drive', 'v3', credentials=creds)
                        self.sheets_service = build('sheets', 'v4', credentials=creds)
                        
                        progress_bar.progress(1.00)
                        status_text.text("Authentication successful!")
                        
                        # Clear the code from URL
                        st.query_params.clear()
                        return True
                    except Exception as e:
                        st.error(f"Authentication failed: {str(e)}")
                        return False
                else:
                    # Show authorization link
                    st.markdown("### Google Authentication Required")
                    st.markdown(f"[Authorize with Google]({auth_url})")
                    st.info("Click the link above to authorize, you'll be redirected back automatically")
                    st.stop()
            else:
                st.error("Google credentials missing in Streamlit secrets")
                return False
                
        except Exception as e:
            st.error(f"Authentication failed: {str(e)}")
            return False
    
    def search_emails(self, sender: str = "", search_term: str = "", 
                     days_back: int = 7, max_results: int = 50) -> List[Dict]:
        """Search for emails with attachments"""
        try:
            query_parts = ["has:attachment"]
            
            if sender:
                query_parts.append(f'from:"{sender}"')
            
            if search_term:
                if "," in search_term:
                    keywords = [k.strip() for k in search_term.split(",")]
                    keyword_query = " OR ".join([f'"{k}"' for k in keywords if k])
                    if keyword_query:
                        query_parts.append(f"({keyword_query})")
                else:
                    query_parts.append(f'"{search_term}"')
            
            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            
            query = " ".join(query_parts)
            
            result = self.gmail_service.users().messages().list(
                userId='me', q=query, maxResults=max_results
            ).execute()
            
            messages = result.get('messages', [])
            return messages
            
        except Exception as e:
            st.error(f"Email search failed: {str(e)}")
            return []
    
    def process_gmail_workflow(self, config: dict, progress_bar, status_text, log_container, progress_base=0.0, progress_scale=1.0):
        """Process Gmail attachment download workflow"""
        try:
            status_text.text("Starting Gmail workflow...")
            self._log_message("Starting Gmail workflow...")
            
            # Search for emails
            emails = self.search_emails(
                sender=config['sender'],
                search_term=config['search_term'],
                days_back=config['days_back'],
                max_results=config['max_results']
            )
            
            progress_bar.progress(progress_base + 0.25 * progress_scale)
            self._log_message(f"Gmail search completed. Found {len(emails)} emails")
            
            if not emails:
                self._log_message("No emails found matching criteria")
                return {'success': True, 'processed': 0}
            
            status_text.text(f"Found {len(emails)} emails. Processing attachments...")
            
            # Create base folder in Drive
            base_folder_name = "Gmail_Attachments_BigBasket"
            base_folder_id = self._create_drive_folder(base_folder_name, config.get('gdrive_folder_id'))
            
            if not base_folder_id:
                error_msg = "Failed to create base folder in Google Drive"
                self._log_message(f"ERROR: {error_msg}")
                st.error(error_msg)
                return {'success': False, 'processed': 0}
            
            progress_bar.progress(progress_base + 0.50 * progress_scale)
            
            processed_count = 0
            total_attachments = 0
            
            for i, email in enumerate(emails):
                try:
                    status_text.text(f"Processing email {i+1}/{len(emails)}")
                    
                    # Get email details
                    email_details = self._get_email_details(email['id'])
                    subject = email_details.get('subject', 'No Subject')[:50]
                    sender = email_details.get('sender', 'Unknown')
                    
                    self._log_message(f"Processing email: {subject} from {sender}")
                    
                    # Get full message
                    message = self.gmail_service.users().messages().get(
                        userId='me', id=email['id'], format='full'
                    ).execute()
                    
                    if not message or not message.get('payload'):
                        continue
                    
                    # Extract attachments
                    attachment_count = self._extract_attachments_from_email(
                        email['id'], message['payload'], email_details, config, base_folder_id, log_container
                    )
                    
                    total_attachments += attachment_count
                    if attachment_count > 0:
                        processed_count += 1
                        self._log_message(f"Found {attachment_count} attachments in: {subject}")
                    
                    progress = 0.50 + (i + 1) / len(emails) * 0.45
                    progress_bar.progress(progress_base + progress * progress_scale)
                    
                except Exception as e:
                    error_msg = f"Failed to process email {email.get('id', 'unknown')}: {str(e)}"
                    self._log_message(f"ERROR: {error_msg}")
            
            progress_bar.progress(progress_base + 1.00 * progress_scale)
            final_msg = f"Gmail workflow completed! Processed {total_attachments} attachments from {processed_count} emails"
            status_text.text(final_msg)
            self._log_message(f"SUCCESS: {final_msg}")
            
            return {'success': True, 'processed': total_attachments}
            
        except Exception as e:
            error_msg = f"Gmail workflow failed: {str(e)}"
            self._log_message(f"ERROR: {error_msg}")
            st.error(error_msg)
            return {'success': False, 'processed': 0}
    
    def process_excel_workflow(self, config: dict, progress_bar, status_text, log_container, progress_base=0.0, progress_scale=1.0):
        """Process Excel GRN workflow from Drive files"""
        try:
            status_text.text("Starting Excel GRN workflow...")
            self._log_message("Starting Excel GRN workflow...")
            
            # Get Excel files from Drive folder
            excel_files = self._get_excel_files(config['excel_folder_id'], config['max_results'])
            
            progress_bar.progress(progress_base + 0.25 * progress_scale)
            self._log_message(f"Found {len(excel_files)} Excel files")
            
            if not excel_files:
                msg = "No Excel files found in the specified folder"
                self._log_message(msg)
                return {'success': True, 'processed': 0}
            
            status_text.text(f"Found {len(excel_files)} Excel files. Processing...")
            
            # Read existing sheet data
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=config['spreadsheet_id'],
                range=f"{config['sheet_name']}!A:ZZ"
            ).execute()
            values = result.get('values', [])
            
            if values:
                headers = values[0]
                rows = values[1:]
                df_existing = pd.DataFrame(rows, columns=headers)
                df_existing = self._clean_dataframe(df_existing)
                if "Item Code" in df_existing.columns and "po_number" in df_existing.columns:
                    existing_keys = set(zip(df_existing["Item Code"], df_existing["po_number"]))
                else:
                    existing_keys = set()
                    self._log_message("Warning: 'Item Code' or 'po_number' columns not found in existing sheet")
            else:
                df_existing = pd.DataFrame()
                existing_keys = set()
            
            sheet_has_headers = not df_existing.empty
            is_first_append = True
            processed_count = 0
            
            for i, file in enumerate(excel_files):
                try:
                    status_text.text(f"Processing Excel file {i+1}/{len(excel_files)}: {file['name']}")
                    self._log_message(f"Processing: {file['name']}")
                    
                    # Read Excel file with robust parsing
                    df = self._read_excel_file_robust(file['id'], file['name'], config['header_row'], log_container)
                    
                    if df.empty:
                        self._log_message(f"SKIPPED - No data extracted from {file['name']}")
                        continue
                    
                    df = self._clean_dataframe(df)
                    
                    if "Item Code" not in df.columns or "po_number" not in df.columns:
                        self._log_message(f"SKIPPED - Missing key columns in {file['name']}")
                        continue
                    
                    # Dedup within new data
                    df = df.drop_duplicates(subset=["Item Code", "po_number"], keep="first")
                    
                    # Filter out existing duplicates
                    new_keys = list(zip(df["Item Code"], df["po_number"]))
                    mask = [key not in existing_keys for key in new_keys]
                    df_unique = df.iloc[mask]
                    
                    if df_unique.empty:
                        self._log_message(f"SKIPPED - No new unique data in {file['name']}")
                        continue
                    
                    self._log_message(f"Data shape: {df_unique.shape} - Columns: {list(df_unique.columns)[:3]}{'...' if len(df_unique.columns) > 3 else ''}")
                    
                    # Append to Google Sheet
                    append_headers = is_first_append and not sheet_has_headers
                    self._append_to_sheet(
                        config['spreadsheet_id'], 
                        config['sheet_name'], 
                        df_unique, 
                        append_headers,
                        log_container
                    )
                    
                    # Update existing keys
                    added_keys = set(zip(df_unique["Item Code"], df_unique["po_number"]))
                    existing_keys.update(added_keys)
                    
                    self._log_message(f"APPENDED {len(df_unique)} unique rows from {file['name']}")
                    processed_count += 1
                    is_first_append = False
                    sheet_has_headers = True
                    
                    progress = 0.25 + (i + 1) / len(excel_files) * 0.75
                    progress_bar.progress(progress_base + progress * progress_scale)
                    
                except Exception as e:
                    error_msg = f"Failed to process Excel file {file.get('name', 'unknown')}: {str(e)}"
                    self._log_message(f"ERROR: {error_msg}")
            
            # Remove duplicates and clean sheet
            if processed_count > 0:
                status_text.text("Removing duplicates from Google Sheet...")
                self._log_message("Removing duplicates from Google Sheet...")
                self._remove_duplicates_from_sheet(
                    config['spreadsheet_id'], 
                    config['sheet_name']
                )
            
            progress_bar.progress(progress_base + 1.00 * progress_scale)
            final_msg = f"Excel workflow completed! Processed {processed_count} files"
            status_text.text(final_msg)
            self._log_message(f"SUCCESS: {final_msg}")
            
            return {'success': True, 'processed': processed_count}
            
        except Exception as e:
            error_msg = f"Excel workflow failed: {str(e)}"
            self._log_message(f"ERROR: {error_msg}")
            st.error(error_msg)
            return {'success': False, 'processed': 0}
    
    def _log_message(self, message: str, log_container=None):
        """Add timestamped message to log storage"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        if 'logs' not in st.session_state:
            st.session_state.logs = []
        
        log_entry = f"[{timestamp}] {message}"
        st.session_state.logs.append(log_entry)
        
        # Keep only last 1000 log entries
        if len(st.session_state.logs) > 1000:
            st.session_state.logs = st.session_state.logs[-1000:]
    
    def _get_email_details(self, message_id: str) -> Dict:
        """Get email details including sender and subject"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='metadata'
            ).execute()
            
            headers = message['payload'].get('headers', [])
            
            details = {
                'id': message_id,
                'sender': next((h['value'] for h in headers if h['name'] == "From"), "Unknown"),
                'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)"),
                'date': next((h['value'] for h in headers if h['name'] == "Date"), "")
            }
            
            return details
            
        except Exception as e:
            return {'id': message_id, 'sender': 'Unknown', 'subject': 'Unknown', 'date': ''}
    
    def _create_drive_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> str:
        """Create a folder in Google Drive"""
        try:
            # Check if folder already exists
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"
            
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            
            if files:
                return files[0]['id']
            
            # Create new folder
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            
            folder = self.drive_service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            
            return folder.get('id')
            
        except Exception as e:
            st.error(f"Failed to create folder {folder_name}: {str(e)}")
            return ""
    
    def _sanitize_filename(self, filename: str) -> str:
        """Clean up filenames"""
        cleaned = re.sub(r'[<>:"/\\|?*]', '_', filename)
        if len(cleaned) > 100:
            name_parts = cleaned.split('.')
            if len(name_parts) > 1:
                extension = name_parts[-1]
                base_name = '.'.join(name_parts[:-1])
                cleaned = f"{base_name[:95]}.{extension}"
            else:
                cleaned = cleaned[:100]
        return cleaned
    
    def _extract_attachments_from_email(self, message_id: str, payload: Dict, sender_info: Dict, config: dict, base_folder_id: str, log_container) -> int:
        """Extract Excel attachments from email"""
        processed_count = 0
        
        if "parts" in payload:
            for part in payload["parts"]:
                processed_count += self._extract_attachments_from_email(
                    message_id, part, sender_info, config, base_folder_id, log_container
                )
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            filename = payload.get("filename", "")
            
            # Filter for Excel files only
            if not filename.lower().endswith(('.xls', '.xlsx', '.xlsm')):
                return 0
            
            try:
                # Get attachment data
                attachment_id = payload["body"].get("attachmentId")
                att = self.gmail_service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=attachment_id
                ).execute()
                
                file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))
                
                # Create folder structure
                sender_email = sender_info.get('sender', 'Unknown')
                if "<" in sender_email and ">" in sender_email:
                    sender_email = sender_email.split("<")[1].split(">")[0].strip()
                
                sender_folder_name = self._sanitize_filename(sender_email)
                type_folder_id = self._create_drive_folder(sender_folder_name, base_folder_id)
                
                # Upload file
                clean_filename = self._sanitize_filename(filename)
                final_filename = f"{message_id}_{clean_filename}"
                
                file_metadata = {
                    'name': final_filename,
                    'parents': [type_folder_id]
                }
                
                media = MediaIoBaseUpload(
                    io.BytesIO(file_data),
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                
                self.drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                
                self._log_message(f"Uploaded Excel file: {filename}")
                processed_count += 1
                
            except Exception as e:
                self._log_message(f"ERROR processing attachment {filename}: {str(e)}")
        
        return processed_count
    
    def _get_excel_files(self, folder_id: str, max_results: int = 100) -> List[Dict]:
        """Get Excel files from Drive folder"""
        try:
            query = (f"'{folder_id}' in parents and "
                    f"(mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or "
                    f"mimeType='application/vnd.ms-excel')")
            
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name)",
                orderBy='createdTime desc',
                pageSize=max_results
            ).execute()
            
            files = results.get('files', [])
            return files
            
        except Exception as e:
            st.error(f"Failed to get Excel files: {str(e)}")
            return []
    
    def _read_excel_file_robust(self, file_id: str, filename: str, header_row: int, log_container) -> pd.DataFrame:
        """Robust Excel file reader with multiple fallback strategies"""
        try:
            # Download file
            request = self.drive_service.files().get_media(fileId=file_id)
            file_stream = io.BytesIO()
            downloader = MediaIoBaseDownload(file_stream, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            file_stream.seek(0)
            self._log_message(f"Attempting to read {filename} (size: {len(file_stream.getvalue())} bytes)")
            
            # Try openpyxl first
            try:
                file_stream.seek(0)
                if header_row == -1:
                    df = pd.read_excel(file_stream, engine="openpyxl", header=None)
                else:
                    df = pd.read_excel(file_stream, engine="openpyxl", header=header_row)
                if not df.empty:
                    self._log_message("SUCCESS with openpyxl")
                    return self._clean_dataframe(df)
            except Exception as e:
                self._log_message(f"openpyxl failed: {str(e)[:50]}...")
            
            # Try xlrd for older files
            if filename.lower().endswith('.xls'):
                try:
                    file_stream.seek(0)
                    if header_row == -1:
                        df = pd.read_excel(file_stream, engine="xlrd", header=None)
                    else:
                        df = pd.read_excel(file_stream, engine="xlrd", header=header_row)
                    if not df.empty:
                        self._log_message("SUCCESS with xlrd")
                        return self._clean_dataframe(df)
                except Exception as e:
                    self._log_message(f"xlrd failed: {str(e)[:50]}...")
            
            # Try raw XML extraction
            df = self._try_raw_xml_extraction(file_stream, header_row, log_container)
            if not df.empty:
                self._log_message("SUCCESS with raw XML extraction")
                return self._clean_dataframe(df)
            
            self._log_message(f"FAILED - All strategies failed for {filename}")
            return pd.DataFrame()
            
        except Exception as e:
            self._log_message(f"ERROR reading {filename}: {str(e)}")
            return pd.DataFrame()
    
    def _try_raw_xml_extraction(self, file_stream: io.BytesIO, header_row: int, log_container) -> pd.DataFrame:
        """Raw XML extraction for corrupted Excel files"""
        try:
            file_stream.seek(0)
            with zipfile.ZipFile(file_stream, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                shared_strings = {}
                
                # Read shared strings
                shared_strings_file = 'xl/sharedStrings.xml'
                if shared_strings_file in file_list:
                    try:
                        with zip_ref.open(shared_strings_file) as ss_file:
                            ss_content = ss_file.read().decode('utf-8', errors='ignore')
                            string_pattern = r'<t[^>]*>([^<]*)</t>'
                            strings = re.findall(string_pattern, ss_content, re.DOTALL)
                            for i, string_val in enumerate(strings):
                                shared_strings[str(i)] = string_val.strip()
                    except Exception:
                        pass
                
                # Find worksheet
                worksheet_files = [f for f in file_list if 'xl/worksheets/' in f and f.endswith('.xml')]
                if not worksheet_files:
                    return pd.DataFrame()
                
                with zip_ref.open(worksheet_files[0]) as xml_file:
                    content = xml_file.read().decode('utf-8', errors='ignore')
                    cell_pattern = r'<c[^>]*r="([A-Z]+\d+)"[^>]*(?:t="([^"]*)")?[^>]*>(?:.*?<v[^>]*>([^<]*)</v>)?(?:.*?<is><t[^>]*>([^<]*)</t></is>)?'
                    cells = re.findall(cell_pattern, content, re.DOTALL)
                    
                    if not cells:
                        return pd.DataFrame()
                    
                    cell_data = {}
                    max_row = 0
                    max_col = 0
                    
                    for cell_ref, cell_type, v_value, is_value in cells:
                        col_letters = ''.join([c for c in cell_ref if c.isalpha()])
                        row_num = int(''.join([c for c in cell_ref if c.isdigit()]))
                        col_num = 0
                        for c in col_letters:
                            col_num = col_num * 26 + (ord(c) - ord('A') + 1)
                        
                        if is_value:
                            cell_value = is_value.strip()
                        elif cell_type == 's' and v_value:
                            cell_value = shared_strings.get(v_value, v_value)
                        elif v_value:
                            cell_value = v_value.strip()
                        else:
                            cell_value = ""
                        
                        cell_data[(row_num, col_num)] = self._clean_cell_value(cell_value)
                        max_row = max(max_row, row_num)
                        max_col = max(max_col, col_num)
                    
                    if not cell_data:
                        return pd.DataFrame()
                    
                    data = []
                    for row in range(1, max_row + 1):
                        row_data = []
                        for col in range(1, max_col + 1):
                            row_data.append(cell_data.get((row, col), ""))
                        if any(cell for cell in row_data):
                            data.append(row_data)
                    
                    if len(data) < max(1, header_row + 2):
                        return pd.DataFrame()
                    
                    if header_row == -1:
                        headers = [f"Column_{i+1}" for i in range(len(data[0]))]
                        return pd.DataFrame(data, columns=headers)
                    else:
                        if len(data) > header_row:
                            headers = [str(h) if h else f"Column_{i+1}" for i, h in enumerate(data[header_row])]
                            return pd.DataFrame(data[header_row+1:], columns=headers)
                        else:
                            return pd.DataFrame()
                
        except Exception as e:
            return pd.DataFrame()
    
    def _clean_cell_value(self, value):
        """Clean and standardize cell values, preserving numbers"""
        if value is None or value == "":
            return None
        value = str(value).strip().replace("'", "")
        if value == "":
            return None
        try:
            if '.' in value or 'e' in value.lower():
                return float(value)
            return int(value)
        except (ValueError, TypeError):
            return value
    
    def _clean_dataframe(self, df):
        """Clean DataFrame by removing blank rows and duplicates"""
        if df.empty:
            return df
        
        # Remove single quotes from string columns
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.replace("'", "", regex=False)
        
        # Remove rows where second column is blank
        if len(df.columns) >= 2:
            second_col = df.columns[1]
            mask = ~(
                df[second_col].isna() | 
                (df[second_col].astype(str).str.strip() == "") |
                (df[second_col].astype(str).str.strip() == "nan")
            )
            df = df[mask]
        
        # Remove duplicate rows
        original_count = len(df)
        df = df.drop_duplicates()
        duplicates_removed = original_count - len(df)
        
        return df
    
    def _append_to_sheet(self, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame, append_headers: bool, log_container):
        """Append DataFrame to Google Sheet, preserving number types"""
        try:
            # Prepare values with proper types
            def process_value(v):
                if pd.isna(v) or v is None:
                    return ''
                if isinstance(v, (int, float)):
                    return v
                return str(v)
            
            values = []
            if append_headers:
                values.append([str(col) for col in df.columns])  # Headers as strings
            
            # Convert DataFrame rows to lists, preserving numeric types
            for row in df.itertuples(index=False):
                processed_row = []
                for cell in row:
                    processed_row.append(process_value(cell))
                values.append(processed_row)
            
            if not values:
                self._log_message("No data to append to Google Sheet")
                return
            
            # Find the next empty row
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A:A"
            ).execute()
            existing_rows = result.get('values', [])
            start_row = len(existing_rows) + 1 if existing_rows else 1
            
            # Append data
            self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A{start_row}",
                valueInputOption="RAW",
                body={"values": values}
            ).execute()
            
            self._log_message(f"Appended {len(values)} rows to Google Sheet")
            
        except Exception as e:
            self._log_message(f"ERROR appending to sheet: {str(e)}")
            raise
    
    def _remove_duplicates_from_sheet(self, spreadsheet_id: str, sheet_name: str):
        """Remove duplicates based on Item Code and po_number, clean blanks, and sort"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:ZZ"
            ).execute()
            values = result.get('values', [])
            
            if not values:
                self._log_message("Sheet is empty, skipping cleaning")
                return
            
            # Pad all rows to max length
            max_len = max(len(row) for row in values)
            for row in values:
                row.extend([''] * (max_len - len(row)))
            
            # Create headers
            headers = [values[0][i] if values[0][i] else f"Column_{i+1}" for i in range(max_len)]
            
            rows = values[1:]
            df = pd.DataFrame(rows, columns=headers)
            before = len(df)
            
            if "Item Code" in df.columns and "po_number" in df.columns:
                df = df.drop_duplicates(subset=["Item Code", "po_number"], keep="first")
            
            after_dup = len(df)
            removed_dup = before - after_dup
            
            # Clean blanks
            df.replace('', pd.NA, inplace=True)
            df.dropna(how='all', inplace=True)  # blank rows
            df.dropna(how='all', axis=1, inplace=True)  # blank columns
            df.fillna('', inplace=True)
            
            after_clean = len(df)
            removed_clean = after_dup - after_clean
            
            # Sort if po_number present
            if "po_number" in df.columns:
                df = df.sort_values(by="po_number", ascending=True)
            
            # Prepare data for update, preserving numeric types
            def process_value(v):
                if pd.isna(v) or v == '':
                    return ''
                try:
                    if '.' in str(v) or 'e' in str(v).lower():
                        return float(v)
                    return int(v)
                except (ValueError, TypeError):
                    return str(v)
            
            values = []
            values.append([str(col) for col in df.columns])  # Headers as strings
            for row in df.itertuples(index=False):
                values.append([process_value(cell) for cell in row])
            
            # Update sheet
            self.sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=sheet_name
            ).execute()
            
            body = {"values": values}
            self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="RAW",
                body=body
            ).execute()
            
            self._log_message(f"Cleaned sheet: removed {removed_dup} duplicates and {removed_clean} blank rows")
                
        except Exception as e:
            self._log_message(f"ERROR cleaning sheet: {str(e)}")

def create_streamlit_ui():
    """Create the Streamlit user interface"""
    st.title("🛒 BigBasket Automation")
    st.markdown("### Automated Gmail Attachment Processing & Excel GRN Consolidation")
    
    # Initialize automation object
    if 'automation' not in st.session_state:
        st.session_state.automation = BigBasketAutomation()
    
    # Initialize logs
    if 'logs' not in st.session_state:
        st.session_state.logs = []
    
    # Sidebar for authentication and configuration
    st.sidebar.title("Navigation")
    
    # Authentication section
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔐 Authentication")
    
    if st.sidebar.button("Authenticate Google APIs", key="auth_button"):
        with st.spinner("Authenticating..."):
            progress_bar = st.progress(0.0)
            status_text = st.empty()
            
            success = st.session_state.automation.authenticate_from_secrets(
                progress_bar, status_text
            )
            
            if success:
                st.sidebar.success("✅ Authentication successful!")
                st.session_state.authenticated = True
            else:
                st.sidebar.error("❌ Authentication failed")
                st.session_state.authenticated = False
    
    # Check authentication
    if not st.session_state.get('authenticated', False):
        st.warning("⚠️ Please authenticate with Google APIs first using the sidebar")
        st.stop()
    
    st.sidebar.success("✅ Authenticated")
    
    # Configuration in sidebar
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ⚙️ Configuration")
    
    days_back = st.sidebar.number_input(
        "Days Back to Search",
        min_value=1,
        max_value=365,
        value=2,
        help="How many days back to search emails"
    )
    
    max_results = st.sidebar.number_input(
        "Maximum Results",
        min_value=1,
        max_value=1000,
        value=1000,
        help="Maximum number of emails/files to process"
    )
    
    header_row = st.sidebar.selectbox(
        "Header Row Position",
        options=[0, 1, 2, -1],
        format_func=lambda x: "First row (0)" if x == 0 else "Second row (1)" if x == 1 else "Third row (2)" if x == 2 else "No headers (-1)",
        help="Row number where headers are located (-1 means no headers)"
    )
    
    # Show hardcoded configurations
    with st.sidebar.expander("📋 Hardcoded Configuration", expanded=False):
        st.markdown("**Gmail Configuration:**")
        st.code("""
Sender: bbnet2@bigbasket.com
Search Term: grn
Gmail Drive Folder: 1l5L9IdQ8WcV6AZ04JCeuyxvbNkLPJnHt
        """)
        
        st.markdown("**Excel Configuration:**")
        st.code("""
Excel Source Folder: 1fdio9_h28UleeRjgRnWF32S8kg_fgWbs
Target Spreadsheet: 170WUaPhkuxCezywEqZXJtHRw3my3rpjB9lJOvfLTeKM
Sheet Name: bbalertgrn_2
Duplicate Check: Based on Item Code + po_number
        """)
    
    # Hardcoded configurations
    gmail_config = {
        'sender': 'bbnet2@bigbasket.com',
        'search_term': 'grn',
        'days_back': days_back,
        'max_results': max_results,
        'gdrive_folder_id': '1l5L9IdQ8WcV6AZ04JCeuyxvbNkLPJnHt'
    }
    
    excel_config = {
        'excel_folder_id': '1fdio9_h28UleeRjgRnWF32S8kg_fgWbs',
        'spreadsheet_id': '170WUaPhkuxCezywEqZXJtHRw3my3rpjB9lJOvfLTeKM',
        'sheet_name': 'bbalertgrn_2',
        'header_row': header_row,
        'max_results': max_results
    }
    
    # Create tabs for workflows and logs
    tab_gmail, tab_excel, tab_combined, tab_logs = st.tabs(["📧 Gmail to Drive", "📊 Drive to Sheets", "🔄 Combined Workflow", "📋 Activity Logs"])
    
    with tab_gmail:
        st.markdown("### 🚀 Execute Gmail to Drive Workflow")
        if st.button("Run Gmail to Drive", type="primary"):
            progress_bar = st.progress(0.0)
            status_text = st.empty()
            
            result = st.session_state.automation.process_gmail_workflow(
                gmail_config, progress_bar, status_text, None
            )
            
            if result['success']:
                st.success(f"✅ Gmail workflow completed! Processed {result['processed']} attachments")
            else:
                st.error("❌ Gmail workflow failed")
    
    with tab_excel:
        st.markdown("### 🚀 Execute Drive to Sheets Workflow")
        if st.button("Run Drive to Sheets", type="primary"):
            progress_bar = st.progress(0.0)
            status_text = st.empty()
            
            result = st.session_state.automation.process_excel_workflow(
                excel_config, progress_bar, status_text, None
            )
            
            if result['success']:
                st.success(f"✅ Excel workflow completed! Processed {result['processed']} files")
            else:
                st.error("❌ Excel workflow failed")
    
    with tab_combined:
        st.markdown("### 🚀 Execute Combined Workflow")
        if st.button("Run Combined Workflow", type="primary"):
            progress_bar = st.progress(0.0)
            status_text = st.empty()
            
            # Run Gmail workflow (first half of progress)
            gmail_result = st.session_state.automation.process_gmail_workflow(
                gmail_config, progress_bar, status_text, None, progress_base=0.0, progress_scale=0.5
            )
            
            if gmail_result['success']:
                # Run Excel workflow (second half of progress)
                excel_result = st.session_state.automation.process_excel_workflow(
                    excel_config, progress_bar, status_text, None, progress_base=0.5, progress_scale=0.5
                )
                
                if excel_result['success']:
                    st.success(f"✅ Combined workflow completed! Processed {gmail_result['processed']} attachments and {excel_result['processed']} files")
                else:
                    st.error("❌ Combined workflow failed in Excel processing")
            else:
                st.error("❌ Combined workflow failed in Gmail processing")
    
    with tab_logs:
        st.markdown("### 📋 Activity Logs")
        
        # Log controls
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            if st.button("🗑️ Clear Logs", use_container_width=True):
                st.session_state.logs = []
                st.rerun()
        
        with col2:
            auto_refresh = st.checkbox("Auto Refresh", value=False)
        
        with col3:
            log_level = st.selectbox(
                "Filter by Level",
                ["All", "ERROR", "SUCCESS", "INFO"],
                index=0
            )
        
        # Display logs
        st.markdown("---")
        
        if auto_refresh:
            # Auto-refresh every 2 seconds when enabled
            time.sleep(2)
            st.rerun()
        
        # Filter logs based on selection
        filtered_logs = st.session_state.logs
        if log_level != "All":
            filtered_logs = [log for log in st.session_state.logs if log_level in log]
        
        if filtered_logs:
            # Display logs in a scrollable text area
            logs_text = '\n'.join(filtered_logs)
            st.text_area(
                "Detailed Activity Log",
                value=logs_text,
                height=500,
                key="main_log_display",
                help="All workflow activities are logged here with timestamps"
            )
            
            # Show log statistics
            st.markdown("#### 📈 Log Statistics")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_logs = len(st.session_state.logs)
                st.metric("Total Entries", total_logs)
            
            with col2:
                error_count = len([log for log in st.session_state.logs if "ERROR" in log])
                st.metric("Errors", error_count)
            
            with col3:
                success_count = len([log for log in st.session_state.logs if "SUCCESS" in log])
                st.metric("Success", success_count)
            
            with col4:
                recent_logs = len(st.session_state.logs[-10:])
                st.metric("Recent (Last 10)", recent_logs)
                
        else:
            st.info("No logs available. Run a workflow to see activity logs here.")
    
    # Instructions in sidebar
    with st.sidebar.expander("📖 Instructions", expanded=False):
        st.markdown("""
        ### How to Use This App
        
        1. **Authentication**: Click the "Authenticate Google APIs" button
        2. **Configuration**: Adjust parameters as needed
        3. **Execution**: Go to the desired tab and click the run button
           - **Gmail to Drive**: Downloads Excel attachments from Gmail to Google Drive
           - **Drive to Sheets**: Processes Excel files from Drive and appends unique data to Google Sheets
           - **Combined Workflow**: Runs both sequentially
        4. **Monitor**: Check the "Activity Logs" tab for details
        
        ### Workflow Details
        
        **Gmail to Drive:**
        - Searches emails from bbnet2@bigbasket.com containing "grn"
        - Downloads Excel attachments to Drive
        - Organizes by sender
        
        **Drive to Sheets:**
        - Processes Excel files from source folder
        - Extracts data robustly
        - Appends only unique rows (based on Item Code + po_number)
        - Preserves number types
        
        ### Troubleshooting
        
        - Refresh page if authentication fails
        - Check logs for parsing issues
        - Ensure Sheet permissions
        """)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: gray;'>
        BigBasket Automation App | Built with Streamlit
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    create_streamlit_ui()
