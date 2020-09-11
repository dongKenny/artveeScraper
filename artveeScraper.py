from bs4 import BeautifulSoup
import requests
import csv
import math
import re
import os
import json
import logging
import boto3
from botocore.exceptions import ClientError

def create_bucket(bucket_name, region=None):
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def upload_file(file_name, bucket, s3, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        response = s3.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def create_json(csv_path, json_path):

    """
    Args:
        csv_path : file path for the csv
        json_path: file path for the json
    """

    data = {} 
      
    with open(csv_path, encoding='utf-8') as csvf: 
        csv_reader = csv.DictReader(csvf) 
          
        # Convert each row into a dictionary and add it to data 
        for rows in csv_reader: 
            key = rows['Title'] 
            data[key] = rows 
  
    #json.dumps() function to dump data 
    with open(json_path, 'w', encoding='utf-8') as jsonf: 
        jsonf.write(json.dumps(data, indent=4)) 

def scrape_meta_images(url, category, dataPath, writer, s3):

    """
    Args:  
        url (str): URL for the paginated category pages
        category (str): The category used in the url
        dataPath (str): The path where the csv, json, and temporary images will be stored
        writer: Writes the appended elements in data to the csv
        s3 (botocore.client.S3): s3 client through which images are uploaded
    """

    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    cards = soup.find_all("div", {"class" : re.compile("product-grid-item product woodmart-hover-tiled*")})
    imgSource = soup.find_all("a", {"class" : "product-image-link linko"})

    #img_index refers to the image of the current card of the 48
    #card refers to the div containing the image and its title/artist

    img_index = 0
    for card in cards:
        data = []

        #Formatted in nested if-statements to prevent receiving an error for a missing element/class (None type)
        title = card.find("h3", class_="product-title")
        if (title != None):
            if (title.find("a") != None):
                title = title.get_text()
                data.append(title)
        else:
            title = "Untitled"
            data.append(title)

        artist_info = card.find("div", class_="woodmart-product-brands-links")
        if (artist_info != None):
            artist_info = artist_info.get_text()
            data.append(artist_info)
        else:
            artist_info = "Unknown"
            data.append(artist_info)

        
        # Find the download page in the card using the href
        # Parse with soup to find the href which contains the download link to the image itself
        # Write the image locally, upload to s3 bucket, remove the image

        img_dl_page = requests.get(imgSource[img_index].get("href"))
        img_soup = BeautifulSoup(img_dl_page.content, "html.parser")
        img_link = img_soup.find("a", {"class" : "prem-link gr btn btn-secondary dis snax-action snax-action-add-to-collection snax-action-add-to-collection-downloads"}).get("href")
        img_name = title + ".jpg"
        img_path = os.path.join(dataPath, img_name)

        with open(img_path, "wb") as img_file:
            img_file.write(requests.get(img_link).content)

            with open(img_path, "rb") as s3_img:
                s3.upload_fileobj(s3_img, "artvee", title + ".jpg")
            s3_img.close()

            img_file.close()


        os.remove(img_path)
        data.append(category)
        writer.writerow(data)
        img_index += 1

def pageCounter(category):

    """
    Args:
        category : The category used in the url

    Explanation:
        Parse first page of a category
        Find number of results displayed on page
        Have 48 results displayed, mod 48, and add 1 for any remainder
        Return total pages to iterate
    """

    url = "https://artvee.com/c/%s/page/1/?per_page=48" % category
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    results = soup.find("p", class_="woocommerce-result-count").text.strip("results").strip()
    no_pages = math.floor(int(results) / 48)

    if (int(results) % 48 > 0):
        no_pages += 1
    
    return no_pages

if __name__ == "__main__":
    s3 = boto3.client('s3')
    create_bucket("artvee", "us-west-1")
    data_path = ""
    csv_path = os.path.join(data_path, "artvee.csv")
    json_path = os.path.join(data_path + "artvee.json")
            
    with open(csv_path, "w", newline = "", encoding="utf-8") as f:

        if (data_path == ""):
            print("\nError: Please assign a value to the data_path \n")
            f.close()

        headers = ["Title", "Artist", "Category"]
        writer = csv.writer(f)
        writer.writerow(headers)

        categories = ["abstract", "figurative", "landscape", "religion", "mythology", "posters", "animals", "illustration", "fashion", "still-life", "historical", "botanical", "drawings", "japanese-art"]

        for category in categories:
            no_pages = pageCounter(category)

            #Pagination
            for p in range(1, no_pages + 1):
                print("Currently looking at: %s, page %d" % (category, p))
                url = "https://artvee.com/c/%s/page/%d/?per_page=48" % (category, p)
                scrape_meta_images(url, category, data_path, writer, s3)

        f.close()

    create_json(csv_path, json_path)
    
    with open(json_path, "rb") as s3_meta:
        s3.upload_fileobj(s3_meta, "artvee", "artveeMeta.json")
