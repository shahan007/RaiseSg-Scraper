#---------------- imports ----------------------
from .generate_header import RotateHeader
from lxml import html
import requests as req
import threading
import json
import re
import pandas as pd
#------------------------ ----------------------

class RaiseScraper:
    
    __base_url = "https://www.raise.sg"
        
    def __init__(self):
        self.__rotate_header = RotateHeader()
        self.__strip_spaces  = self._regex_stripper()   
        self.__data = {"SocialCos":[]}
        self.__threads = []
        
    def request_tree(self,link):
        header = self.__rotate_header.fetch_header()
        res = req.get(link,headers=header)
        tree = html.fromstring(html=res.text)
        return tree

    def get_next_page_link(self,tree):
        expression = "//*[@class='active']/following-sibling::li[1]/a/@href"
        next_link  = tree.xpath(expression)
        if next_link:
            return next_link[0] 
        return None

    def get_each_page_item_links(self,tree):
        expression = "//div[contains(@class,'list-dir-even')]/div[@class='row']//div[@class='infor-content']/a/@href"
        item_links_array = tree.xpath(expression)
        if item_links_array:
            return item_links_array
        return None
    
    def scrape_item(self,tree):
        #company title
        co_expression = "//div[@class='content']/h3[@class='title']/text()"
        #returns relative src link for image
        img_expression = "//div[@class='logo']/img/@src"
        #about us
        abt_us_expression = "//div[@class='service']/h3[@class='title' and text()='About Us']/following-sibling::div[1]//text()"
        #products & services
        p_s_expression = "//div[@class='service']/h3[@class='title' and (contains(text(),'Products') or contains(text(),'Services'))]/following-sibling::div[1]//text()"
        #email
        email_expression = "//div[@class='note']/a/text()"
        
        data = {
            "Company" : co_expression,
            "Email"   : email_expression,
            "Image"   :img_expression,
            "About Us": abt_us_expression,
            "Products_And_Services":p_s_expression 
        }                                            
        
        for k,v in data.items():
            value = tree.xpath(v)        
            if len(value)>1:
                data[k] = self.__strip_spaces.sub("","".join(value)).strip()
            elif len(value) == 1:
                data[k] = value[0]
            else:
                data[k] = ""
        if data["Image"]:
            data["Image"] = self._convert_rel_to_abs_link(data["Image"])
        return data        
    
    def scrape_items(self,item_abs_links):
        for item_link in item_abs_links:            
            tree = self.request_tree(item_link)
            data = self.scrape_item(tree)            
            self.__data["SocialCos"].append(data)
            
    
    def start_scraping(self):
        print("\n\nstarting scraping ! Please wait for it to end ! \n\n")
        currentUrl = "https://www.raise.sg/directory/directories/default.html"
        while currentUrl:                                    
            tree = self.request_tree(currentUrl)            
            currentPageLinkArray = self.get_each_page_item_links(tree)                        
            if currentPageLinkArray:
                currentPageLinkArray = self._convert_array_rel_to_abs_links(currentPageLinkArray)                
                thread = threading.Thread(target=lambda: self.scrape_items(currentPageLinkArray))                
                thread.start()
                self.__threads.append(thread)
            currentUrl = self.get_next_page_link(tree)                             
            if currentUrl:
                currentUrl = self._convert_rel_to_abs_link(currentUrl)                                                        
        for thread in self.__threads:
            thread.join()        
        print("\n\nscraping ended !\n\n")           
    
    def save_to_file(self,path):
        print("\n\n---Running Data Saving Dont Quit !---\n\n")
        with open(path,mode='w') as outfile:
            json.dump(self.__data, outfile, indent=4)
            print("\n\n---File Saved---\n\n")
            print("\nExited Program\n")
            
    def convert_fjson_to_fcsv(self,fileIn,fileOut):
        """
        fileIn  ---> path to json file which contains scraped data
        fileOut ---> path to csv file which will contain the scraped data
        """
        print("File conversion from json->csv occuring !")
        with open(fileIn,mode='r') as infile:
            json_data = json.load(infile)["SocialCos"]    
            df = pd.json_normalize(json_data)    
            df = df.sort_values(by=["Company"]).reset_index(drop=True)
            df.to_csv(fileOut,index=False)            
        print(f"File successfully converted from json -> csv found here {fileOut} !")
            
    def _regex_stripper(self):
        regex_strip_expression = r'[\n\r\t\xa0]'
        return re.compile(regex_strip_expression)        
    
    def _convert_rel_to_abs_link(self,link):
        return req.compat.urljoin(type(self).__base_url, link)
    
    def _convert_array_rel_to_abs_links(self,linkArray):
        """
        Transform the relative links to absolute links
        """
        return [ self._convert_rel_to_abs_link(link) for link in linkArray]    