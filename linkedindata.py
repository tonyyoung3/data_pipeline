from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import time
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import csv

service = Service(executable_path='msedgedriver.exe')
options = webdriver.EdgeOptions()
driver = webdriver.Edge(options=options)
driver.maximize_window()


#inline-notification__text  #end You've viewed all jobs for this search

 #('https://ca.linkedin.com/jobs/search?keywords=internship&location=Vancouver')


def get_url(location):
    base = "https://ca.linkedin.com/jobs/search?keywords=Internship&location="
    final = base + location
    return final

#driver.get("https://ca.linkedin.com/jobs/search?keywords=Internship%20data&location=vancouver")

def get_main(url):
    time.sleep(3)  
    driver.get(url)
    jobs_list = driver.find_elements( By.CLASS_NAME, "base-search-card__info")
    while len(driver.find_element(By.CLASS_NAME,'inline-notification__text').text) < 3 :
        time.sleep(5)
        driver.execute_script("arguments[0].scrollIntoView();", jobs_list[-1])
        jobs_list = driver.find_elements( By.CLASS_NAME, "base-search-card__info")
        buttons = driver.find_elements( By.CLASS_NAME, "base-card__full-link")
        
        # print(len(jobs_list))
        # print(driver.find_element( By.CLASS_NAME, "inline-notification__text").text)
        
        try:
            button = driver.find_element(By.CLASS_NAME,'infinite-scroller__show-more-button')
            button.click()
            time.sleep(3)
        except :
            pass
    return buttons, jobs_list

def get_detail(p):
    bottons, jobs_list = get_main(p)
    list = []
    for i,j in zip(bottons,jobs_list):  
        role = j.find_element(By.CLASS_NAME,"base-search-card__title" )
        companies = j.find_element(By.CLASS_NAME,"base-search-card__subtitle" )
        locations = j.find_element(By.CLASS_NAME,"job-search-card__location" )
        timepost = j.find_element(By.CLASS_NAME,"base-search-card__metadata" )
        t = timepost.find_element(By.TAG_NAME,"time")
        i.click()
        time.sleep(2)
        texts = driver.find_element(By.CLASS_NAME, "show-more-less-html__markup").text
        detail = driver.find_elements(By.CLASS_NAME, "description__job-criteria-text")
        
        try:
            level, job_type, job_func, industry = detail
        except:
            print(f"url {i.get_attribute('href')} can not be processed")
            continue
            

        my_dict = {'role':role.text,'name' : companies.text, 'location': locations.text, 'post_time': t.get_attribute('datetime'), 'description' : texts, 'level' : level.text, 'job_type': job_type.text, 'job_func' : job_func.text, 'industry' : industry.text, "url" : i.get_attribute('href') }
        list.append(my_dict)      
        
    return list

def write_file(province, data):
    csv_file = province+'.csv'
    with open(csv_file, 'w', newline='') as file:
        #print(data[0])
        filedname = data[0].keys()
        #print(filedname)
        writer = csv.DictWriter(file, fieldnames=filedname)
        writer.writeheader()
        writer.writerows(data)
        

province = ['Alberta','Quebec']

for p in province:
    list = get_detail(get_url(p))
    write_file(p,list)
    print('p' + 'ends')

time.sleep(10)
driver.quit()


