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

def create_bucket(bucketName, region=None):
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucketName)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucketName,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def upload_file(fileName, bucket, objectName = None):
      # If S3 object_name was not specified, use file_name
    if objectName is None:
        objectName = fileName

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(fileName, bucket, objectName)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def createJSON(csvPath, jsonPath):
    data = {} 
      
    with open(csvPath, encoding='utf-8') as csvR: 
        csvReader = csv.DictReader(csvR) 
          
        # Convert each row into a dictionary and add it to data 
        for rows in csvReader: 
            key = rows['Title'] 
            data[key] = rows 
  
    #json.dumps() function to dump data 
    with open(jsonPath, 'w', encoding='utf-8') as jsonW: 
        jsonW.write(json.dumps(data, indent=4)) 

def getMeta(url, category):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    cards = soup.find_all("div", {"class" : re.compile("product-grid-item product woodmart-hover-tiled*")})
    imgSource = soup.find_all("a", {"class" : "product-image-link linko"})

    #imgIndex refers to the image on the page, tracks which one is being downloaded of the 48
    imgIndex = 0

    #A card refers to the div containing the image and its title/artist
    for card in cards:
        data = []

        #Formatted in nested if statements to prevent receiving an error for a missing element/class
        title = card.find("h3", class_="product-title")
        if (title != None):
            if (title.find("a") != None):
                title = title.get_text()
                data.append(title)
        else:
            title = "Untitled"
            data.append(title)

        artistInfo = card.find("div", class_="woodmart-product-brands-links")
        if (artistInfo != None):
            artistInfo = artistInfo.get_text()
            data.append(artistInfo)
        else:
            artistInfo = "Unknown"
            data.append(artistInfo)

        #Get the download page from a card, use the soup parser to find the download link for the image
        imgdlPage = requests.get(imgSource[imgIndex].get("href"))
        imgSoup = BeautifulSoup(imgdlPage.content, "html.parser")
        imgLink = imgSoup.find("a", {"class" : "prem-link gr btn btn-secondary dis snax-action snax-action-add-to-collection snax-action-add-to-collection-downloads"}).get("href")
        imgName = title + ".jpg"
        imgPath = os.path.join(dataPath, imgName)

        with open(imgPath, "wb") as imgFile:
            imgFile.write(requests.get(imgLink).content)
            #Upload image to artvee bucket
            with open(imgPath, "rb") as s3Img:
                s3.upload_fileobj(s3Img, "artvee", title + ".jpg")
            s3Img.close()
            imgFile.close()

        #Remove locally downloaded image after uploading it and closing streams
        os.remove(imgPath)
        data.append(category)
        writer.writerow(data)
        imgIndex += 1

s3 = boto3.client('s3')
dataPath = ""               #INSERT THE PATH FOR THE CSV/JSON/TEMPORARY IMAGES
csvPath = os.path.join(dataPath, "artvee.csv")

with open(csvPath, "w", newline = "", encoding="utf-8") as f:

    if (dataPath == ""):
        print("\nError: Please change the dataPath variable on line 109\n")
        f.close()

    writer = csv.writer(f)
    headers = ["Title", "Artist", "Category"]
    writer.writerow(headers)

    categories = ["abstract", "figurative", "landscape", "religion", "mythology", "posters", "animals", "illustration", "fashion", "still-life", "historical", "botanical", "drawings", "japanese-art"]

    for category in categories:
        #Parse first page of a category, find number of results, mod 48 and add 1 for any remainder to get the total page numbers to iterate
        url = "https://artvee.com/c/%s/page/1/?per_page=48" % category
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        results = soup.find("p", class_="woocommerce-result-count").text.strip("results").strip()
        noPages = math.floor(int(results) / 48)

        if (int(results) % 48 > 0):
            noPages += 1

        for p in range(1, noPages + 1):
            print("Currently looking at: %s, page %d" % (category, p))
            url = "https://artvee.com/c/%s/page/%d/?per_page=48" % (category, p)
            getMeta(url, category)

    f.close()

jsonPath = dataPath + "/artvee.json"
createJSON(csvPath, jsonPath)

#Create s3 bucket, upload json
create_bucket("artvee", "us-west-1")
#response = s3.list_buckets()
with open(jsonPath, "rb") as s3Meta:
    s3.upload_fileobj(s3Meta, "artvee", "artveeMeta.json")