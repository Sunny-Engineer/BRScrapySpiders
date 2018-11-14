import scrapy
from scrapy.crawler import CrawlerProcess
import re
import base64
import json
import requests
from urllib.parse import   quote
import boto3
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import logging
import os
import uuid

# Bid Retriever API Libraries
import br_api
import AmazonLib


class BaseSpider(scrapy.Spider):

    s3 = boto3.resource('s3')
    data = {}
    def __init__(self):
        logger = logging.getLogger()
        logger.info("About to dispatch in Base Spider")
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def download_file(self, response):
        logger = logging.getLogger()
        logger.info("About to dispatch in Download File")

        folder_name = response.meta['folder_name']
        origin_path = response.url
        file_name = response.meta['file_name']

        logger.info("folder_name = {}" .format(folder_name))
        logger.info("origin_path = {}".format(origin_path))
        logger.info("file_name = {}".format(file_name))

        self.s3.Bucket(self.settings.attributes['BUCKET_NAME'].value).put_object(Key=folder_name + '/' + file_name, Body=response.body)
        self.data['project_files'].append(
            {'folder_name': folder_name, 'file_name': file_name, 'original_path': origin_path})
        #
        #  Add logic here to write 925 record to DynamoDB
        try:
            # this is the AWS Account for Development
            aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
            aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
            aws_region = os.environ.get("AWS_REGION")
            aws_session_token = os.environ.get("aws_session_token")

            bucket_name = os.environ.get("BR_TEMP_VAULT")

            logger.info("aws_access_key_id = " + aws_access_key_id)
            logger.info("aws_secret key =" + aws_secret_access_key)
            logger.info("aws Region =" + aws_region)

            # Create a session and resources to write Use case information to AWS
            aws_session = AmazonLib.create_S3_session(aws_region, aws_access_key_id, aws_secret_access_key, aws_session_token)
            s3_resource = AmazonLib.create_S3_resource(aws_session)
            dynamo_resource = AmazonLib.create_dynamo_resource(aws_session)

            #initialize fields needed by API
            bucket_name = self.settings.attributes['BUCKET_NAME'].value

            # Open the table object,  This should have a try catch around it!
            try:
                tablename = "925FilePreprocessing"
                newdytable = AmazonLib.open_table(dynamo_resource, tablename)
            except Exception as e:
                logger.exception(e)
                logger.info("The Open Table Call for {} failed.".format(tablename))

            # write a record into the Preprocessing WIP table.
            # Define parameters for 925 write
            params925 = {}
            params925['source_file_id'] = str(uuid.uuid4())

            # params925['doc_id'] = doc_id - This is not set until the record is processed by the 925 processor
            #params925['doc_type'] = doc_type
            # params925['file_id'] = file_id - This is not set until the record is processed by the 925 processor
            #params925['file_key'] = sourcekey
            params925['file_original_filename'] = file_name
            params925['original_filepath'] = origin_path

            params925['project_id'] = "123"
            params925['project_name'] = "Test project_name"
            # params925['sequence_num'] = sequence_num
            #params925['source_system'] = source_system
            #params925['submission_id'] = submission_id
            #params925['submission_datetime'] = submission_datetime
            #params925['submitter_email'] = submitter_email
            #params925['user_timezone'] = user_timezone
            params925['vault_bucket'] = bucket_name

            params925['process_status'] = 'queued'
            #params925['create_datetime'] = submission_datetime
            #params925['edit_datetime'] = submission_datetime
            params925['process_attempts'] = 0

            AmazonLib.write_925item(newdytable, params925)
            logger.info('-------  925 WIP Table write completed  ---------------------')

        except Exception as e:
            logger.exception(e)
            logger.info("Use Case Builder Failed:  local File= {},   S3 Destination = {}".format(bucket_name, file_name))


    def clean_text(self , ptext):
        logger = logging.getLogger()
        logger.info("About to dispatch in clean_text")
        ptext = ptext.replace('\n' , ' ')
        return ptext.strip()

    def spider_closed(self, spider):
        logger = logging.getLogger()
        logger.info(self.data)


# --------------  Process Gradebeam Projects -----------------------------
class GradebeamSpider(BaseSpider):

    name = 'gradebeam'
    url = "https://www1.gradebeam.com/bidresponse/e2l0YmlkOjEzNDI2NjAzMyxvcmdpZDo5NzA5NDUsaXRicmVzcG9uc2U6MCx1c2VyaWQ6MH0="


    formdata  = []
    def start_requests(self):
        logger = logging.getLogger()
        yield scrapy.Request(url=self.url, callback=self.parse , dont_filter=True)
        logger.info("The URL being scraped  = {}".format(self.url))

    def parse(self , response):
        logger = logging.getLogger()
        keystrings = re.findall(r"\/([^\/]+)$", self.url)
        if len(keystrings) == 0:
            self.data['status'] = "Invalid the URL"
            return
        base64filekey = base64.b64decode(keystrings[0])
        decoded_string = base64filekey.decode("utf-8")
        itbids = re.findall(r"itbid:([0-9]+)", decoded_string)
        if len(itbids) == 0 :
            self.data['status'] = "Invalid the URL"
            return
        else:
            itbid = itbids[0]
        orgids = re.findall(r"orgid:([0-9]+)", decoded_string)
        if len(orgids) == 0:
            self.data['status'] = "Invalid the URL"
            return
        else:
            orgid = orgids[0]
        url = "https://www1.gradebeam.com/dataservices/itb/get/{itbid}/{orgid}".format(itbid = itbid , orgid = orgid)
        yield scrapy.Request(url = url, callback=self.parse_data , dont_filter=True , meta = {'orgid':orgid})

    def parse_data(self , response):
        logger = logging.getLogger()
        data = json.loads(response.body_as_unicode())
        self.data['project_number'] = data['Itb']['ProjectID']
        self.data['project_name'] =  data['Itb']['ProjectName']
        self.data['project_address1'] = data['Itb']['Address1']
        self.data['project_city'] = data['Itb']['City']
        self.data['project_state'] = data['Itb']['State']
        self.data['project_zip'] = data['Itb']['Zip']
        self.data['project_bid_datetime'] =  data['Itb']['BidDueDateString']+' '+ data['Itb']['BidDueTimeString']
        self.data['project_desc'] =data['Itb']['Description']
        self.data['project_files'] = []
        self.data['status'] ="Success"
        package_id = data['Itb']['PackageID']
        modifiedBy = data['Itb']['ModifiedBy']
        url  = "https://www.gradebeam.com/Attachment/FolderView.aspx"
        form_data  = {
            "pid": str(package_id),
            "uid": str(modifiedBy) ,
            "oid": str(response.meta['orgid']),
            "fn": ".. / Attachment / FolderView.aspx"
        }
        self.formdata = form_data

        # Write the attributes to a project data record using the API
        try:
            logger.info("No project was found, so about to create a new project.")

            # If no project is found, create a new one...
            create_project_params = {}

            create_project_params["project_name"] =self.data['project_name']
            create_project_params["project_admin_user_id"] = "Admin User ID From Spider!"
            create_project_params["project_address1"] = self.data['project_address1']
            create_project_params["project_city"] = self.data['project_city']
            create_project_params["project_state"] = self.data['project_state']
            create_project_params["project_zip"] = self.data['project_zip']
            create_project_params["project_desc"] = self.data['project_desc']
            create_project_params["project_number"] = self.data['project_number']
            create_project_params["project_bid_datetime"] = self.data['project_bid_datetime']

            create_project_results = br_api.br_CreateProjectDL(create_project_params)

            logger.info("  Project Created with Following Params - {}".format(create_project_params))

            create_project_status = create_project_results['status']
            logger.info("The br_CreateProjectDL call status = " + str(create_project_status))

            if create_project_status == "failed":
                logger.info("create project failed.  Params = " + str(create_project_params))
            else:
                project_id = create_project_results['project_id']

        except Exception as e:
            logger.info('Creating a new project for this submission failed:  ' + str(e))
            logger.info("----------------------------------------------------------------------------------------")
        else:
            logger.info("New project created for submission.  project_id ='" + project_id + "'.")
            logger.info("----------------------------------------------------------------------------------------")


        yield  scrapy.FormRequest(url = url ,method="POST" , formdata=form_data, callback = self.parse_file ,dont_filter=True)



    def parse_file(self , response):
        logger = logging.getLogger()
        body_string = response.body_as_unicode()
        ctl00_radScriptMgr_TSMs = re.findall(r'_TSM_HiddenField_=ctl00_radScriptMgr_TSM&amp;compress=1&amp;_TSM_CombinedScripts_=([^\"]+)' ,body_string )
        ctl00_radStyleSheetManager_TSSMs = re.findall(r"hf\.value \+= '([^\']+)" ,body_string )
        if len(ctl00_radScriptMgr_TSMs)==0 or len(ctl00_radStyleSheetManager_TSSMs)==0:
            return
        for idx , folder in enumerate(response.xpath('//div[@id="ctl00_MainContentPlaceHolder_radTVFolders"]/ul/li/div/span[2]/text()').extract()):
           headers = {
                'Accept': "*/*",
                'Accept-Encoding': "gzip, deflate, br",
                'Accept-Language': "en-US,en;q=0.9",
                'Cache-Control': "no-cache",
                'Connection': "keep-alive",
                'Content-Type': "application/x-www-form-urlencoded",
                'Host': "www.gradebeam.com",
                'Origin': "https://www.gradebeam.com",
                'Referer': "https://www.gradebeam.com/Attachment/FolderView.aspx",
                'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
                'X-MicrosoftAjax': "Delta=true"
            }
           formdata = {
               'ctl00_radScriptMgr_TSM' : ctl00_radScriptMgr_TSMs[0],
               'ctl00_radStyleSheetManager_TSSM' : quote(ctl00_radStyleSheetManager_TSSMs[0]),
               'ctl00MainContentPlaceHolderhdnpackageid' :self.formdata['pid'],
                'ctl00MainContentPlaceHolderhdnuserid' : self.formdata['uid'],
               'ctl00MainContentPlaceHolderhdnorganizationid' :self.formdata['oid'],
               'EVENTARGUMENT' : str(idx),
               'VIEWSTATE' : quote(response.xpath('//input[@id="__VIEWSTATE" ]/@value').extract_first()),
               'VIEWSTATEGENERATOR' : quote(response.xpath('//input[@id="__VIEWSTATEGENERATOR" ]/@value').extract_first()),
               'EVENTVALIDATION' : quote(response.xpath('//input[@id="__EVENTVALIDATION" ]/@value').extract_first())
           }
           payload = "ctl00%24radScriptMgr=ctl00%24MainContentPlaceHolder%24ctl00%24MainContentPlaceHolder%24radTVFoldersPanel%7Cctl00%24MainContentPlaceHolder%24radTVFolders" \
                     "&ctl00_radScriptMgr_TSM="+formdata['ctl00_radScriptMgr_TSM']+"&ctl00_radStyleSheetManager_TSSM="+formdata['ctl00_radStyleSheetManager_TSSM']+\
                     "&ctl00%24MainContentPlaceHolder%24hdnpackageid="+formdata['ctl00MainContentPlaceHolderhdnpackageid']+\
                     "&ctl00%24MainContentPlaceHolder%24hdnuserid="+formdata['ctl00MainContentPlaceHolderhdnuserid']+\
                     "&ctl00%24MainContentPlaceHolder%24hdnorganizationid="+formdata['ctl00MainContentPlaceHolderhdnorganizationid']+\
                     "&ctl00%24MainContentPlaceHolder%24radFileStatusFilter=Active%20Files&ctl00_MainContentPlaceHolder_radFileStatusFilter_ClientState=%7B%22logEntries%22%3A%5B%5D%2C%22value%22%3A%22Active%22%2C%22text%22%3A%22Active%20Files%22%2C%22enabled%22%3Atrue%2C%22checkedIndices%22%3A%5B%5D%2C%22checkedItemsTextOverflows%22%3Afalse%7D&ctl00_MainContentPlaceHolder_radGFiles_ClientState=%7B%22selectedIndexes%22%3A%5B%5D%2C%22reorderedColumns%22%3A%5B%5D%2C%22expandedItems%22%3A%5B%5D%2C%22expandedGroupItems%22%3A%5B%5D%2C%22expandedFilterItems%22%3A%5B%5D%2C%22deletedItems%22%3A%5B%5D%2C%22hidedColumns%22%3A%5B%5D%2C%22showedColumns%22%3A%5B%5D%2C%22scrolledPosition%22%3A%2295%2C0%22%2C%22popUpLocations%22%3A%7B%7D%2C%22draggedItemsIndexes%22%3A%5B%5D%7D&ctl00_MainContentPlaceHolder_radTVFolders_ClientState=%7B%22expandedNodes%22%3A%5B%5D%2C%22collapsedNodes%22%3A%5B%5D%2C%22logEntries%22%3A%5B%5D%2C%22selectedNodes%22%3A%5B%220%22%5D%2C%22checkedNodes%22%3A%5B%220%22%5D%2C%22scrollPosition%22%3A0%7D&ctl00_MainContentPlaceHolder_DocumentFax_ClientState=&ctl00_MainContentPlaceHolder_UploadDocuments_ClientState=&ctl00_MainContentPlaceHolder_radWindowMgr_ClientState=&__EVENTTARGET=ctl00%24MainContentPlaceHolder%24radTVFolders&__EVENTARGUMENT=%7B%22commandName%22%3A%22Click%22%2C%22index%22%3A%22"+formdata['EVENTARGUMENT']+\
                     "%22%7D&__VIEWSTATE="+formdata['VIEWSTATE']+ \
                     "&__VIEWSTATEGENERATOR="+formdata['VIEWSTATEGENERATOR']+\
                     "&__EVENTVALIDATION="+formdata['EVENTVALIDATION']+\
                     "&__ASYNCPOST=true&RadAJAXControlID=ctl00_MainContentPlaceHolder_radAjaxManager"
           url = "https://www.gradebeam.com/Attachment/FolderView.aspx"
           result = requests.request("POST", url, data=payload, headers=headers)
           files = re.findall(r"javascript:DownloadAttachment\(\'([^\']+)','([^\']+)'",result.text)
           for item in files:
               url = "https://www1.gradebeam.com//documentsgc/PARDownload?cloudKey=" + item[0]
               yield scrapy.Request(url=url, callback=self.parse_file_download,
                                    meta={'folder_name': folder, 'file_name': item[1]},
                                    dont_filter=True)


    def parse_file_download(self , response):
        logger = logging.getLogger()
        body_string = response.body_as_unicode()
        logger.info ("output = {}" .format())
        yield scrapy.Request(url=body_string.replace('"',''), callback=self.download_file, meta={'file_name': response.meta['file_name'],'folder_name': "{name}/{projectid}/{folder_name}".format(
            name=self.name,
            projectid=self.data['Project Number'],
            folder_name=response.meta['folder_name'])})

# --------------  Process Pipeline Suite Projects -----------------------------
class PipelineSuiteSpider(scrapy.Spider):
    logger = logging.getLogger()  # you cannot add a name here or it blows up??????
    logger.setLevel(logging.DEBUG)

    name = 'pipelinesuite'
    url = "https://fortneyweygandt.pipelinesuite.com/ehPipelineSubs/dspProject/projectID/118431"
    projectID = "94505"
    securityKey = "G3qNxcCb9"
    logger.info("Pipeline URL = {}".format(url))
    logger.info("Pipeline Security Key = {}" .format(securityKey))

    def start_requests(self):
        logger = logging.getLogger()
        yield scrapy.Request(url=self.url, callback=self.parse)

    def parse(self, response):
        logger = logging.getLogger()
        nexts = re.findall(r"\/([^\/]+)$" , response.url)
        if len(nexts) == 0:
            self.data['status'] = "Invalid URL!"
            return
        next = nexts[0]
        form_data = {
            'next': next,
            'portalProjectID': self.projectID,
            'portalSecurityKey': self.securityKey
        }
        url  = "https://fortneyweygandt.pipelinesuite.com/ehPipelineSubs/login/"
        yield scrapy.http.FormRequest(url=url, method='POST', formdata=form_data,
                                      callback=self.parse_login ,  dont_filter = True)

    def parse_login(self , response):
        logger = logging.getLogger()
        if response.url != self.url:
            self.data['status'] = "Invalid Api Key!"
            return
        self.data['project_number'] = response.xpath('//th[text()="Project #"]/following-sibling::td/text()').extract_first()
        self.data['project_name'] = self.clean_text(response.xpath('//th[contains(text(),"Project Name")]/following-sibling::td/text()').extract_first())
        self.data['project_address1'] = self.clean_text(
            response.xpath('//th[contains(text(),"Address")]/following-sibling::td/text()[2]').extract_first())
        self.data['project_city'] = self.clean_text(
            response.xpath('//th[contains(text(),"City")]/following-sibling::td/text()').extract_first())
        self.data['project_state'] = self.clean_text(
            response.xpath('//th[contains(text(),"State")]/following-sibling::td/text()').extract_first())
        self.data['project_zip'] = self.clean_text(
            response.xpath('//th[contains(text(),"Zip")]/following-sibling::td/text()').extract_first())
        self.data['project_bid_datetime'] = self.clean_text(
            response.xpath('//th[contains(text(),"Bid Date")]/following-sibling::td/text()').extract_first())+" "+self.clean_text(
            response.xpath('//th[contains(text(),"Bid Time")]/following-sibling::td/text()').extract_first())
        self.data['project_desc'] =' '.join(response.xpath('//th[contains(text(),"Scope")]/following-sibling::td/*/text()').extract())
        self.data['project_files'] = []
        self.data['status'] ="Success"


        try:
            logger.info("No project was found, so about to create a new project.")

            # If no project is found, create a new one...
            create_project_params = {}

            create_project_params["project_name"] =self.data['project_name']
            create_project_params["project_admin_user_id"] = "Admin User ID From Spider!"
            create_project_params["project_address1"] = self.data['project_address1']
            create_project_params["project_city"] = self.data['project_city']
            create_project_params["project_state"] = self.data['project_state']
            create_project_params["project_zip"] = self.data['project_zip']
            create_project_params["project_desc"] = self.data['project_desc']
            create_project_params["project_number"] = self.data['project_number']
            create_project_params["project_bid_datetime"] = self.data['project_bid_datetime']

            create_project_results = br_api.br_CreateProjectDL(create_project_params)

            logger.info("  Project Created with Following Params - {}".format(create_project_params))

            create_project_status = create_project_results['status']
            logger.info("The br_CreateProjectDL call status = " + str(create_project_status))

            if create_project_status == "failed":
                logger.info("create project failed.  Params = " + str(create_project_params))
            else:
                project_id = create_project_results['project_id']

        except Exception as e:
            logger.info('Creating a new project for this submission failed:  ' + str(e))
            logger.info("----------------------------------------------------------------------------------------")
        else:
            logger.info("New project created for submission.  project_id ='" + project_id + "'.")
            logger.info("----------------------------------------------------------------------------------------")


        for folder in response.xpath('//div[@id="opr_files"]/ul/li'):
           folder_name  = folder.xpath('./@data-text').extract_first()
           files  =folder.xpath('./ul/li/@data-file-path').extract()
           for file in files:
               file_name  =file.split('/')[-1]
               yield scrapy.Request(url='https://'+file, callback=self.download_file , meta={'file_name':file_name , 'folder_name':"{name}/{projectid}/{folder_name}".format(name = self.name , projectid =self.projectID , folder_name = folder_name )})


def br_scrape(soucesystem , url, username, password):
    logger = logging.getLogger()
    # this routine is called from the 920 lambda routine, or other external applications and passes the information necessary to scrape the site.
    try:

        process = CrawlerProcess({'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'  })

        if soucesystem == "pipelinesuite":
            process.crawl(PipelineSuiteSpider)

        elif soucesystem == "gradebeam":
            process.crawl(GradebeamSpider)

        process.start() # the script will block here until the crawling is finished

    except Exception as e:
        logger.info("The br_scrape routine failed with error = {} " .format(e))

if __name__ == "__main__":
# Define error logging
    try:
        #  https://docs.python.org/3.6/howto/logging-cookbook.html
        logger = logging.getLogger()  # you cannot add a name here or it blows up??????
        logger.setLevel(logging.DEBUG)

    except Exception as e:
        logger.info(" error = {}" .format(e))

    try:
        url = "https://www1.gradebeam.com/bidresponse/e2l0YmlkOjEzNDI2NjAzMyxvcmdpZDo5NzA5NDUsaXRicmVzcG9uc2U6MCx1c2VyaWQ6MH0="
        br_scrape("gradebeam", url, "", "")
        logger.info("About to scrape pipeline")

        url = "https://fortneyweygandt.pipelinesuite.com/ehPipelineSubs/dspProject/projectID/118431"
        br_scrape("pipelinesuite", url, "", "")


    except Exception as e:
        logger.exception(" error = {}".format(e))