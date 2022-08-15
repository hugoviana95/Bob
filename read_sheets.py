from __future__ import print_function

import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def read_sheets(id, range, valueRenderOption):  #'FORMATTED_VALUE' ou 'UNFORMATTED_VALUE'
    # If modifying these scopes, delete the file token.json.
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token_sheets.json'):
        creds = Credentials.from_authorized_user_file('token_sheets.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token_sheets.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId = id,
            range = range,
            valueRenderOption = valueRenderOption
            ).execute()
        
        return(result['values'])

    except HttpError as err:
        print('\nOcorreu algum erro ao ler a planilha online\n')
        print(err)


if __name__ == '__main__':
    range = 'Report1'
    id = '16hb0aB_QQ2g7o6DNRgAVLHejXFFAimQsfFEnaMhSQiM'

    read_sheets(id, range)