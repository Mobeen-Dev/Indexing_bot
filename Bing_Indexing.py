# Import necessary libraries
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import time

# Define website credentials provided by the client
api_key = 'YOUR_API_KEY'
api_password = 'YOUR_API_PASSWORD'
store_url = "YOUR_STORE_URL"  # Backend URL of the store
store_name = "YOUR_STORE_NAME"
store_front_name = "YOUR_STORE_FRONT_NAME"

# Function to fetch IDs from Google Sheets
def fetch_id_from_webhook():
    # Credentials data should be obtained from the Google Developer Console
    credentials_data = {
        # Insert your credentials here
    }
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_data, scope)
    gc = gspread.authorize(credentials)

    # Open the Google Sheet by its title
    spreadsheet_title = 'DataBase_WEBHOOK'
    sh = gc.open(spreadsheet_title)

    # Select the worksheet by title
    worksheet_title = 'digilog'
    worksheet = sh.worksheet(worksheet_title)

    # Get all values from the second column
    column_values = worksheet.col_values(2)[0:]  # Assuming there is no header in the first row

    # Remove duplicates and store in a list
    distinct_values = list(set(column_values))

    # Clear all data in the worksheet
    worksheet.clear()

    # Create a list of Cell objects
    cells = worksheet.range('B1:B{}'.format(len(distinct_values)))

    # Update each cell with the corresponding value
    for i, cell in enumerate(cells):
        cell.value = distinct_values[i]

    # Update the worksheet in a single batch
    worksheet.update_cells(cells)

    # Print the number of items in the list
    print("Number of items in the list:", len(distinct_values))
    
    # Return the distinct values
    if distinct_values:
        return distinct_values


# Function to fetch product handle using API
def product_handle(product_id=None):
    global api_key, api_password, store_name

    try:
        # Build the URL
        url = f'https://{store_name}/admin/api/2024-01/products/{product_id}.json'

        # Set up authentication headers
        header = {
            "Content-Type": "application/json",
        }

        auth = (api_key, api_password)

        # Make the API request
        response_ = requests.get(url, headers=header, auth=auth)

        # Check if the request was successful
        if response_.status_code == 200:
            # Parse the JSON response
            product_data = response_.json()

            # Extract the product handle
            product_handle = product_data["product"]["handle"]
            if product_data["product"]['status'] == 'active':
                product_url = f"https://{store_front_name}/products/{product_handle}"
                print(product_url)
                return product_url
        else:
            print(f"Error: {response_.status_code} - {response_.text}")
    except requests.exceptions.RequestException as e:
        print(f"RequestException: {e}")
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


# Function to index URLs
def indexing_Urls(product_urls=None):
    if product_urls:
        global store_front_name
        headers = {
            'Content-Type': 'application/json',
        }
        batch_size = 225
        batches = [product_urls[i:i + batch_size] for i in range(0, len(product_urls), batch_size)]
        total_indexed = 0  # Variable to store the total count of indexed URLs

        for batch in batches:
            data = {
                "siteUrl": f"http://www.{store_front_name}",
                "urlList": batch
            }

            response = requests.post(
                'https://ssl.bing.com/webmaster/api.svc/json/SubmitUrlbatch?apikey=YOUR_BING_API_KEY',
                headers=headers, data=json.dumps(data))

            if response.status_code == 200:
                indexed_count = len(batch)
                total_indexed += indexed_count  # Update the total count
                print(f'{indexed_count} URLs submitted successfully')
            else:
                print(f"Error: {response.status_code} - {response.text}")
                break  # Stop the loop if there's an error

        return total_indexed  # Return the total count of indexed URLs


# Function to update database
def Update_database(row_no):
    # JSON key file should be obtained from the Google Developer Console
    JSON_KEY_FILE = {
        # Insert your JSON key file data here
    }

    # Define spreadsheet and worksheet names
    SPREADSHEET_NAME = 'DataBase_WEBHOOK'
    WORKSHEET_NAME = 'digilog'

    # Authorize and authenticate with Google Sheets API
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(JSON_KEY_FILE, scope)
    gc = gspread.authorize(credentials)

    # Open the Google Sheets document by name
    spreadsheet = gc.open(SPREADSHEET_NAME)

    # Choose the specific worksheet by name
    worksheet = spreadsheet.worksheet(WORKSHEET_NAME)

    # Get all values from the second column
    column_values = worksheet.col_values(2)[0:]  # Assuming there is not a header in the first row

    # Remove duplicates and store in a list
    distinct_values = list(set(column_values))

    # Create a list of Cell objects for the range above the special code cell
    cells = worksheet.range(f'B1:B{row_no}')

    # Clear the content in each cell
    for cell in cells:
        cell.value = ''

    # Update the worksheet with the cleared values in a single batch
    worksheet.update_cells(cells)

    # Get all values from the second column
    column_values = worksheet.col_values(2)[0:]  # Assuming there is not a header in the first row

    # Remove duplicates and store in a list
    distinct_values = list(set(column_values))

    # Clear all data in the worksheet
    worksheet.clear()

    # Create a list of Cell objects
    cells = worksheet.range('B1:B{}'.format(len(distinct_values)))

    # Update each cell with the corresponding value
    for i, cell in enumerate(cells):
        cell.value = distinct_values[i]

    # Update the worksheet in a single batch
    worksheet.update_cells(cells)
    print(f"Values above row number {row_no} have been cleared.")


# Main part of the code
last_id = None
try:
    # Fetch distinct IDs from webhook
    distinct_id = fetch_id_from_webhook()
    last_row_no = None
    
    product_front_url = []
    # Fetch product handle for each distinct ID
    for ID in distinct_id:
        product_front_url.append(product_handle(ID))
        time.sleep(2)

    if product_front_url:
        # Index URLs
        last_row_no = indexing_Urls(product_front_url)

    print(last_row_no)
    # Update database
    Update_database(last_row_no)

except gspread.exceptions.GSpreadException as e:
    print(f"GSpreadException: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
