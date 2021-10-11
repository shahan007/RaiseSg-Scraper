#------------ imports -------------------
from RaiseSgScraper import RaiseScraper
#----------------------------------------

def main():
    raiseScraper = RaiseScraper()
    raiseScraper.start_scraping()
    filepath = "./Output/data.json"
    # saving to json file
    raiseScraper.save_to_file(filepath)
    #converting and saving to csv
    raiseScraper.convert_fjson_to_fcsv(filepath,"./Output/data.csv")

if __name__ == "__main__":
    main()