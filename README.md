# artveeScraper
Scrapes every image on artvee.com and collects the metadata in a json from a converted csv; the final json and images are uploaded to an aws s3 bucket.

Using BeautifulSoup4 and requests, I collect the artworks using the categories under the Browse section. 

I parse the page to find the number of results, display 48 artworks per page, and calculate the number of pages using the floor of (results/48) + 1 if there is a remainder.

On each page, I write the metadata (Title, Artist, Nationality, Year, etc.) of the images to csv. I then access the download link from the displayed image. After downloading an image, I upload the image file to the Amazon S3 bucket and delete the locally stored image to save space. 

Once they are all scraped and uploaded, I write the csv to a json and upload that to the s3 bucket
