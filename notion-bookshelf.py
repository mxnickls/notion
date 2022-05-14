import pandas as pd
import requests
import json
import math
#google search for the book using google books API
def google_book_search(search_terms):
    url = 'https://www.googleapis.com/books/v1/volumes?q='
    response = requests.get(url+search_terms, verify = False)
    # load data using Python JSON module
    r = response.content
    data = json.loads(r)
    # Normalizing data
    df = pd.json_normalize(data, record_path =['items'])
    first_row = df.iloc[0]
    first_row_df = df.iloc[:1]
    first_row_df['search_term']=search_terms
    return first_row_df;
secret_key = 'secret_MA3ED4MDVW1ziIEWAkeZ6oHfvjsEkCD64Hntevp7v1E'
database_id = '1b4ac358095e4a0e909f2a59674b1d6e'
def query_databases(secret_key, database_id):
    url = "https://api.notion.com/v1/databases/"+database_id+'/query'
    #print(url)

    payload={'id' : database_id}
    headers = {
        'Notion-Version': '2021-05-13',
        #"Content-Type": "application/json",
        'Authorization': 'Bearer '+secret_key
    }

    response = requests.request("POST", url, headers=headers, data=payload, verify=False)
    print(f"The response code is {response.status_code}")
    if response.status_code != 200:
        raise ApiError(f'Response Status: {response.status_code}')
    else:
        return response.json()
res = query_databases(secret_key, database_id)
#dict_keys(['object', 'results', 'next_cursor', 'has_more'])
res.get('results')
#results are in list format, this has all the items from the database, validate the number of items to make sure all items were read successfully
results = res.get('results')
df = pd.DataFrame([])
print(type(df))
print(len(list(results)))
i=0
while i < len(results):
#each items in results to iterate and convert into dataframe
    page_properties = results[i].get('properties')
    page_keys = page_properties.keys()
    page_values = page_properties.values()

    #get page URL
    page_properties['page_url'] = results[i].get('url')

    #create dataframe
    df_line = pd.DataFrame(list([page_values]), columns=list(page_keys))
    df = df.append(df_line)
    i+=1
df = df.reset_index()
df.shape    
df
#make copy of our dataframe
Notion_data=df
print(f'Shape of dataframe:{Notion_data.shape}')
print(Notion_data.columns)
def clean_link(value):
    try:
        if type(value) is dict:
            value = (value.get('url'))
    except:
        value = 'error'
    return value
#df['Name'].iloc[1].get('title')[0].get('plain_text')
def clean_name(value):
    try:
        if len(value.get('title')) != 0:
            ret_=value.get('title')[0].get('plain_text')
        else:
            ret_= None #value.get('title')
    except:
        ret_ = 'error'
    return ret_
#Notion_data['Author'].iloc[1].get('multi_select')[0].get('name')
def clean_author(value):
    if type(value) is dict:
        try:
            if len(value.get('multi_select')) != 0:
                ret_=value.get('multi_select')[0].get('name')
            else:
                ret_= None #value.get('title')
        except:
            ret_ = 'error'
    else:
        ret_ = value
    return ret_
#not being used
def clean_status(value):
    if type(value) is dict:
        try:
            ret_= value.get('select').get('name')
        except:
            ret_= 'error'
    else:
        ret_ = value
    return ret_
Notion_data['_Name']=Notion_data['Name'].apply(lambda x:clean_name(x))
Notion_data['_Link']=Notion_data['Link'].apply(lambda x:clean_link(x))
Notion_data['_Author']=Notion_data['Author'].apply(lambda x:clean_author(x))
Notion_data['page_id']=Notion_data['page_url'].apply(lambda x: x[-32:])
Notion_data
Notion_data['page_id']
my_list = list(filter(None, Notion_data['_Name']))
my_list_no_nan = []
for i in my_list:
    try:
        if pd.isnull(i)!=True:
            #print('is null')
            my_list_no_nan+=[i]
        #else:
            #print('null')
            #my_list_no_nan += [i]
    except:
        print('except')

print(len(my_list_no_nan))
print(my_list_no_nan)
#my_reading_list = ['Never Split The Difference', "Seeing what others don't", '48 Laws of Power']
#search_terms = 'Never Split The Difference'

info = pd.DataFrame()
for books in my_list_no_nan:
    print(books)
    info = info.append(google_book_search(books))
info.columns
print(info.shape)
google_data = info[['selfLink', 'volumeInfo.title',
       'volumeInfo.subtitle', 'volumeInfo.authors', 'volumeInfo.publisher',
       'volumeInfo.publishedDate', 'volumeInfo.description','volumeInfo.pageCount', 'volumeInfo.categories',
       'volumeInfo.imageLinks.smallThumbnail','volumeInfo.imageLinks.thumbnail','saleInfo.country', 'saleInfo.retailPrice.amount',
       'saleInfo.retailPrice.currencyCode', 'search_term'
     ]]
google_data = (google_data).reset_index()
google_data['volumeInfo.title']
Notion_data
google_data
look_up = pd.merge(Notion_data, google_data, left_on=  ['_Name'], right_on= ['search_term'], how = 'left')
#Update a property on a page based on property type 
def update_page(page_id, property_name, property_type, property_value):
    url = f"https://api.notion.com/v1/pages/{page_id}"

    if property_type =='date':
        property_payload = {
            "start": property_value
        }
    elif property_type =='url':
        property_payload = property_value
    elif property_type =='number':
        property_payload = property_value
    elif property_type =='rich_text':
        property_payload = [{
            "type": "text",
            "text": {
                "content": property_value
        }
        }]
    elif property_type == 'select':
        property_payload = {
            "name": property_value
        }


    payload = json.dumps({
  "properties": {
    property_name: {
      property_type: property_payload
    }
  }
})
    #print(payload)   
    headers = {
      'Content-Type': 'application/json',
      'Notion-Version': '2021-05-13',
      'Authorization': f'Bearer {secret_key}'
    }

    response = requests.request("PATCH", url, headers=headers, data=payload,verify = False)
    print(response.status_code)
    #print(response.json)
    return response.json
def update_properties(property_name, property_type, data_column):
    page_id = look_up['page_id'][i]
    property_value = look_up[data_column][i]
    update_page(page_id, property_name, property_type, property_value)
#Update publishing dates
property_name = 'Publishing/Release Date'
property_type = "date"
data_column = 'volumeInfo.publishedDate'

for i in look_up['page_id'].index:
#if math.isnan(look_up['Publishing/Release Date'][i]):
    update_properties(property_name, property_type, data_column)
#Update Links
property_name = 'Link'
property_type = "url"
data_column = 'selfLink'

for i in look_up['page_id'].index:
   # if math.isnan(look_up['Link'][i]):
    update_properties(property_name, property_type, data_column)
#Update Publishers
property_name = 'Publisher'
property_type = "select"
data_column = 'volumeInfo.publisher'

for i in look_up['page_id'].index:
   # if math.isnan(look_up['Link'][i]):
    update_properties(property_name, property_type, data_column)
#Update Number of pages
property_name = 'Pages'
property_type = "number"
data_column = 'volumeInfo.pageCount'

for i in look_up['page_id'].index:
   # if math.isnan(look_up['Link'][i]):
    update_properties(property_name, property_type, data_column)
#Update Number of Summary
property_name = 'Summary'
property_type = "rich_text"
data_column = 'volumeInfo.description'

for i in look_up['page_id'].index:
   # if math.isnan(look_up['Link'][i]):
    update_properties(property_name, property_type, data_column)
#Update icons for pages
data_column = 'volumeInfo.imageLinks.smallThumbnail'

def update_page_icon(i, data_column, icon_or_cover):

    page_id = look_up['page_id'][i]
    property_value = look_up[data_column][i]

    url = f"https://api.notion.com/v1/pages/{page_id}"

    payload = json.dumps({icon_or_cover: {
        "type": "external",
            "external": {
                "url" : property_value
      }}})
    print(payload)

    headers = {
      'Content-Type': 'application/json',
      'Notion-Version': '2021-05-13',
      'Authorization': f'Bearer {secret_key}'
    }

    response = requests.request("PATCH", url, headers=headers, data=payload,verify = False)
    print(response.status_code)
    print(response.json)
#Update icons for pages
for i in look_up['page_id'].index:
    update_page_icon(i, data_column, 'icon')
#Update cover for pages
for i in look_up['page_id'].index:
    update_page_icon(i, data_column, 'cover')
