import scrapy
from scrapy.crawler import CrawlerProcess
import re
import base64
import json
import requests
from urllib.parse import quote
import boto3
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import logging
import os
import uuid
import coloredlogs
from urllib.parse import urlparse, parse_qs
import br_api
import AmazonLib

logger = logging.getLogger("BRScrapySpiders routine")
coloredlogs.install(logger=logger)


class BaseSpider(scrapy.Spider):

    s3 = boto3.resource('s3')
    create_project_params = {}
    project_attributes = {}
    aws_session = None
    dynamo_resource = None
    bucket_name = None

    def __init__(self):
        coloredlogs.install(logger=self.log)
        self.log.info("About to dispatch in Base Spider")

        aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        aws_region = os.environ.get("AWS_REGION")
        aws_session_token = os.environ.get("aws_session_token")

        self.bucket_name = os.environ.get("BR_TEMP_VAULT")
        logger.info("aws_access_key_id = " + aws_access_key_id)
        logger.info("aws_secret key =" + aws_secret_access_key)
        logger.info("aws Region =" + aws_region)
        logger.info("Bucket Name = {}" .format(self.bucket_name) )

        # Create a session and resources to write Use case information to AWS
        aws_session = AmazonLib.create_S3_session(aws_region, aws_access_key_id, aws_secret_access_key,
                                                  aws_session_token)

        self.dynamo_resource = AmazonLib.create_dynamo_resource(aws_session)
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def download_file(self, response):
        logger = logging.getLogger()
        logger.info("About to dispatch in Download File")

        folder_name = response.meta['folder_name']
        origin_path = response.url
        file_name = response.meta['file_name']

        logger.info("folder_name = {}".format(folder_name))
        logger.info("origin_path = {}".format(origin_path))
        logger.info("file_name = {}".format(file_name))

        self.s3.Bucket(self.bucket_name).put_object(Key=folder_name + '/' + file_name,Body=response.body)
        self.create_project_params['project_files'].append(
            {'folder_name': folder_name, 'file_name': file_name, 'original_path': origin_path})
        #
        #  Add logic here to write 925 record to DynamoDB
        try:
            # Open the table object,  This should have a try catch around it!
            try:
                tablename = "925FilePreprocessing"
                newdytable = AmazonLib.open_table(self.dynamo_resource, tablename)
            except Exception as e:
                logger.exception("The open table call failed. error = {}" ,format(e))
                logger.info("The Open Table Call for {} failed.".format(tablename))

            # write a record into the Preprocessing WIP table.
            # Define parameters for 925 write
            params925 = {}
            params925['source_file_id'] = str(uuid.uuid4())

            # params925['doc_id'] = doc_id - This is not set until the record is processed by the 925 processor
            # params925['doc_type'] = doc_type
            # params925['file_id'] = file_id - This is not set until the record is processed by the 925 processor
            # params925['file_key'] = sourcekey
            params925['file_original_filename'] = file_name
            params925['original_filepath'] = origin_path

            params925['project_id'] = self.project_attributes["project_id"]
            logger.info("project_id from system = {}" .format(params925['project_id']))
            params925['project_name'] = "Test project_name"
            # params925['sequence_num'] = sequence_num
            # params925['source_system'] = source_system
            # params925['submission_id'] = submission_id
            # params925['submission_datetime'] = submission_datetime
            # params925['submitter_email'] = submitter_email
            # params925['user_timezone'] = user_timezone
            params925['vault_bucket'] = self.bucket_name

            params925['process_status'] = 'queued'
            # params925['create_datetime'] = submission_datetime
            # params925['edit_datetime'] = submission_datetime
            params925['process_attempts'] = 0
            AmazonLib.write_925item(newdytable, params925)
            logger.info('-------  925 WIP Table write completed  ---------------------')

        except Exception as e:
            logger.exception(e)
            logger.info(
                "Use Case Builder Failed:  local File= {},   S3 Destination = {}".format(self.bucket_name, file_name))


        except Exception as e:
            # logger.exception(e)
            self.log.error("Use Case Builder Failed:  local File= {},   S3 Destination = {}".format(self.bucket_name, file_name))


    def clean_text(self , ptext):
        ptext = ptext.replace('\n' , ' ')
        return ptext.strip()

    def spider_closed(self, spider):
      if self.create_project_params['status'] == "Success":
        self.log.info('finished Scraping Successfully.....')
        self.log.info("Scraped Result = {}".format(self.create_project_params))
        try:
            self.log.info("No project was found, so about to create a new project.")
            # If no project is found, create a new one...
            create_project_results = br_api.br_CreateProjectDL(self.create_project_params)
            self.log.info("  Project Created with Following Params - {}".format(self.create_project_params))
            create_project_status = create_project_results['status']
            self.log.info("The br_CreateProjectDL call status = " + str(create_project_status))
            if create_project_status == "failed":
                self.log.error("create project failed.  Params = " + str(self.create_project_params))
            else:
                project_id = create_project_results['project_id']
        except Exception as e:
            self.log.error('Creating a new project for this submission failed:  ' + str(e))
            self.log.error("----------------------------------------------------------------------------------------")
        else:
            self.log.info("New project created for submission.  project_id ='" + project_id + "'.")
            self.log.info("----------------------------------------------------------------------------------------")
      else:
          self.log.error('Scraping Failed.....')
          self.log.error(self.create_project_params['status'])


# --------------  Process Gradebeam Projects -----------------------------
class GradebeamSpider(BaseSpider):
    name = 'gradebeam'
    log = logging.getLogger(name)
    formdata  = []

    def __init__(self, url = None , projectID=None, securityKey=None, project_attributes = None):
        self.url = url  # source file name
        self.project_attributes = project_attributes
        super().__init__()
        coloredlogs.install(logger=self.log)
        self.log.info("URL = {}".format(url))
        self.log.info("Project Parameters = {}" .format(project_attributes))
        self.create_project_params['sourceSystem'] = self.name

    def start_requests(self):
        self.log.info("Start scraping....")
        self.log.info("Login the Website....")
        yield scrapy.Request(url=self.url, callback=self.parse , dont_filter=True)

    def parse(self , response):
        keystrings = re.findall(r"\/([^\/]+)$", self.url)
        if len(keystrings) == 0:
            self.create_project_params['status'] = "Invalid the URL"
            return
        try:
          base64filekey = base64.b64decode(keystrings[0])
          decoded_string = base64filekey.decode("utf-8")
        except:
            self.create_project_params['status'] = "Invalid the URL"
            return
        itbids = re.findall(r"itbid:([0-9]+)", decoded_string)
        if len(itbids) == 0 :
            self.create_project_params['status'] = "Invalid the URL"
            return
        else:
            itbid = itbids[0]
        orgids = re.findall(r"orgid:([0-9]+)", decoded_string)
        if len(orgids) == 0:
            self.create_project_params['status'] = "Invalid the URL"
            return
        else:
            orgid = orgids[0]
        self.log.info("Logged in the website successfully....")
        url = "https://www1.gradebeam.com/dataservices/itb/get/{itbid}/{orgid}".format(itbid = itbid , orgid = orgid)
        yield scrapy.Request(url = url, callback=self.parse_data , dont_filter=True , meta = {'orgid':orgid})

    def parse_data(self , response):
        self.log.info("Getting the data from web page....")
        data = json.loads(response.body_as_unicode())
        self.create_project_params["project_number"] = data['Itb']['ProjectID']
        self.create_project_params["project_name"] =  data['Itb']['ProjectName']
        self.create_project_params["project_admin_user_id"] = "Admin User ID From Spider!"
        self.create_project_params["project_address1"] = data['Itb']['Address1']
        self.create_project_params["project_city"] = data['Itb']['City']
        self.create_project_params["project_state"] = data['Itb']['State']
        self.create_project_params["project_zip"] = data['Itb']['Zip']
        self.create_project_params["project_bid_datetime"] =  data['Itb']['BidDueDateString']+' '+ data['Itb']['BidDueTimeString']
        self.create_project_params["project_desc"] =data['Itb']['Description']
        self.create_project_params['project_files'] = []
        self.create_project_params['status'] ="Success"
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
        yield  scrapy.FormRequest(url = url ,method="POST" , formdata=form_data, callback = self.parse_file ,dont_filter=True)

    def parse_file(self , response):
        self.log.info("Getting the file data from web page....")
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
           self.log.info("Getting the file data  from web page....")
           result = requests.request("POST", url, data=payload, headers=headers)
           files = re.findall(r"javascript:DownloadAttachment\(\'([^\']+)','([^\']+)'",result.text)
           for item in files:
               url = "https://www1.gradebeam.com//documentsgc/PARDownload?cloudKey=" + item[0]
               yield scrapy.Request(url=url, callback=self.parse_file_download,
                                    meta={'folder_name': folder, 'file_name': item[1]},
                                    dont_filter=True)


    def parse_file_download(self , response):
        body_string = response.body_as_unicode()
        yield scrapy.Request(url=body_string.replace('"',''), callback=self.download_file, meta={'file_name': response.meta['file_name'],'folder_name': "{name}/{projectid}/{folder_name}".format(
            name=self.name,
            projectid=self.create_project_params["project_number"],
            folder_name=response.meta['folder_name'])})

# --------------  Process Pipeline Suite Projects -----------------------------

class PipelineSuiteSpider(BaseSpider):
    name = 'pipelinesuite'
    log = logging.getLogger(name)
    def __init__(self, url = None , projectID=None, securityKey=None, project_attributes = None):
        self.url = url  # source file name
        self.projectID = projectID
        self.securityKey = securityKey
        self.project_attributes = project_attributes
        super().__init__()
        coloredlogs.install(logger=self.log)
        self.log.info("URL = {}".format(url))
        self.log.info("Project Id  = {}".format(projectID))
        self.log.info("Security Key = {}".format(securityKey))
        self.create_project_params['sourceSystem'] = self.name

    def start_requests(self):
        self.log.info("Start Scraping...")
        yield scrapy.Request(url=self.url, callback=self.parse)

    def parse(self, response):
        nexts = re.findall(r"\/([^\/]+)$" , response.url)
        if len(nexts) == 0:
            self.create_project_params['status'] = "Invalid URL!"
            return
        next = nexts[0]
        form_data = {
            'next': next,
            'portalProjectID': self.projectID,
            'portalSecurityKey': self.securityKey
        }
        self.log.info("Login the website...")
        url  = "https://fortneyweygandt.pipelinesuite.com/ehPipelineSubs/login/"
        yield scrapy.http.FormRequest(url=url, method='POST', formdata=form_data,
                                      callback=self.parse_login ,  dont_filter = True)

    def parse_login(self , response):
        if response.url != self.url:
            self.create_project_params['status'] = "Invalid Project ID and Security Key!"
            return
        self.log.info("Logged in Successfully.")
        self.log.info("Getting the  data  from web page....")
        self.create_project_params["project_number"]= response.xpath('//th[text()="Project #"]/following-sibling::td/text()').extract_first()
        self.create_project_params["project_name"] = self.clean_text(response.xpath('//th[contains(text(),"Project Name")]/following-sibling::td/text()').extract_first())
        self.create_project_params["project_address1"]  = self.clean_text(
            response.xpath('//th[contains(text(),"Address")]/following-sibling::td/text()[2]').extract_first())
        self.create_project_params["project_city"] = self.clean_text(
            response.xpath('//th[contains(text(),"City")]/following-sibling::td/text()').extract_first())
        self.create_project_params["project_state"] = self.clean_text(
            response.xpath('//th[contains(text(),"State")]/following-sibling::td/text()').extract_first())
        self.create_project_params["project_zip"] = self.clean_text(
            response.xpath('//th[contains(text(),"Zip")]/following-sibling::td/text()').extract_first())
        self.create_project_params["project_bid_datetime"] = self.clean_text(
            response.xpath('//th[contains(text(),"Bid Date")]/following-sibling::td/text()').extract_first())+" "+self.clean_text(
            response.xpath('//th[contains(text(),"Bid Time")]/following-sibling::td/text()').extract_first())
        self.create_project_params["project_desc"] =' '.join(response.xpath('//th[contains(text(),"Scope")]/following-sibling::td/*/text()').extract())
        self.create_project_params["project_admin_user_id"] = "Admin User ID From Spider!"
        self.create_project_params['project_files'] = []
        self.create_project_params['status'] ="Success"
        for folder in response.xpath('//div[@id="opr_files"]/ul/li'):
           folder_name  = folder.xpath('./@data-text').extract_first()
           files  =folder.xpath('./ul/li/@data-file-path').extract()
           for file in files:
               file_name  =file.split('/')[-1]
               yield scrapy.Request(url='https://'+file, callback=self.download_file , meta={'file_name':file_name , 'folder_name':"{name}/{projectid}/{folder_name}".format(name = self.name , projectid =self.projectID , folder_name = folder_name )})

# --------------  Process Smartbid Projects -----------------------------

class SmartbidSpider(BaseSpider):
    name = 'smartbid'
    log = logging.getLogger(name)

    def __init__(self, url=None, projectID=None, securityKey=None, project_attributes = None):
        self.url = url  # source file name
        self.project_attributes = project_attributes
        super().__init__()
        coloredlogs.install(logger=self.log)
        self.log.info("URL = {}".format(url))
        self.create_project_params['sourceSystem'] = self.name

    def start_requests(self):
        self.log.info("Start Scraping...")

        url = "https://api.smartinsight.co/token"
        parsed_url = urlparse(self.url)
        params = parse_qs(parsed_url.query)

        self.log.info("Login the website...{}" .format(url))

        try:
            cId = params['cId'][0].replace('bp_' ,'')
            sPassportKey = params['sPassportKey'][0]
            sBidId = params['sBidId'][0]
            form_data = {
                'grant_type': "passport_key",
                'bidid': sBidId,
                'commdetailid': cId,
                'key': sPassportKey,
                'typepassportkey':"bidproject_passportkey"
            }
            yield scrapy.http.FormRequest(url=url, method='POST', formdata=form_data,
                                          callback=self.parse, meta=form_data)
        except:
            self.create_project_params['status'] = "Invalid URL!"

    # parse the site for attribute data
    def parse(self , response):
        self.log.info("Logged in the website successfully....")

        body = response.body_as_unicode()
        json_data = json.loads(body)
        access_token = json_data['access_token']
        account_id =  json_data['account_id']
        url = "https://api.smartinsight.co/api/projects/getbidproject?bidProjectId={bidProjectId}&personId={personId}&bidProjectType=Invited".format(personId = account_id , bidProjectId = response.meta['bidid'] )

        querystring_project_page = {
            'bidProjectId': response.meta['bidid'],
            'personId': account_id,
            'bidProjectType': 'Invited'
        }
        payload_project_page = "------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"grant_type\"\r\n\r\npassport_key" \
                  "\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"bidid\"\r\n\r\n{bidProjectId}" \
                  "\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"commdetailid\"\r\n\r\n{commdetailid}" \
                  "\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\n{key}" \
                  "\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"typepassportkey\"\r\n\r\nbidproject_passportkey" \
                  "\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--"\
            .format(bidProjectId =  response.meta['bidid'], commdetailid = response.meta['commdetailid'] , key = response.meta['key'])

        headers_project_page= {
            'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
            'Content-Type': "application/x-www-form-urlencoded",
            'Authorization': "Bearer {}".format(access_token),
            'Cache-Control': "no-cache",
        }
        response_project_page = requests.request("GET", url, data=querystring_project_page, headers=headers_project_page, params=payload_project_page)
        result_project_page = json.loads(response_project_page.text)
        SystemId =  result_project_page['BidProject']['SystemId']

        self.log.info("Getting the  data  from web page = {}" .format(url))

        self.create_project_params["project_number"] = response.meta['bidid']
        self.create_project_params["project_name"] = result_project_page['BidProject']['Title']
        self.create_project_params["project_address1"] =result_project_page['BidProject']['Address']
        self.create_project_params["project_city"] = result_project_page['BidProject']['City']
        self.create_project_params["project_state"] =result_project_page['BidProject']['State']
        self.create_project_params["project_zip"] = result_project_page['BidProject']['Zip']
        self.create_project_params["project_bid_datetime"] = result_project_page['BidProject']['FullBidDueDate']
        self.create_project_params["project_desc"] = result_project_page['BidProject']['ProjectDescription']
        self.create_project_params['project_files'] = []
        self.create_project_params["project_admin_user_id"] = "Admin User ID From Spider!"
        self.create_project_params['status'] ="Success"

        self.log.info("Getting the file data  from web page....")

        sPlanRoomString = result_project_page['sPlanRoomString']
        fileids = re.findall(r"\(\'onPreviewFile\'\,\s\'([^\']+)" , sPlanRoomString)
        url_file_page = "https://api.smartinsight.co/api/projects/getstackfilelist"
        payload_file_page = "FileId={fileid}&BidProjectId={BidProjectId}&SystemId=590&PersonId={PersonId}&BidProjectType=Invited&ContainerId_DeepZoom=1"\
            .format(BidProjectId = response.meta['bidid'],
                    PersonId = account_id,
                    fileid = fileids[0])
        headers_file_page = {
            'Content-Type': "application/x-www-form-urlencoded",
            'Authorization': "Bearer {}".format(access_token),
            'Cache-Control': "no-cache"
        }
        response_file_page = requests.request("POST", url_file_page, data=payload_file_page, headers=headers_file_page)
        json_data_file_page = json.loads(response_file_page.text)
        for i , item in enumerate(json_data_file_page['lFiles'][:-1]):
             folders = []
             if json_data_file_page['lFiles'][i]['Type'] == "Folder":
                 j = i
                 for n in range(json_data_file_page['lFiles'][i]['Level'], -1, -1):
                     for p in range(j, -1,-1):
                         if json_data_file_page['lFiles'][p]['Type'] == "Folder" and json_data_file_page['lFiles'][p]['Level'] == n:
                             folders.append(json_data_file_page['lFiles'][p]['name'])
                             break
                         j = p

                 folder_name  = '/'.join(reversed(folders))

             if json_data_file_page['lFiles'][i]['Type'] == "File":
                 filename  = json_data_file_page['lFiles'][i]['name']
             else:
                 filename = ''

             if json_data_file_page['lFiles'][i+1]['Type'] == "Plan":
                 sourceId = json_data_file_page['lFiles'][i+1]['sourceId'].replace('_P1', '')
             else:
                 sourceId = ''

             if folder_name != '' and filename != ""  and sourceId != "":
                 filekey = str(sourceId)+'.'+str(response.meta['bidid'])+'.'+str(SystemId)+'.'+'1'
                 base64filekey = base64.b64encode(filekey.encode('ascii'))
                 url_file = "http://smartbidservice.cloudapp.net/DownloadFile.aspx?c="+base64filekey.decode("utf-8") +"&n="+filename

                 yield scrapy.Request(url= url_file, callback=self.download_file,
                                               meta={'file_name':filename , 'folder_name': "{name}/{project_id}/{folder_name}".format(name = self.name , project_id =self.create_project_params["project_number"] , folder_name = folder_name)})

# --------------  Process Building  Connected Projects -----------------------------
class BuildingConnectedSpider(BaseSpider):
    name = 'buildingconnected'
    log = logging.getLogger(name)

    def __init__(self, url = None , projectID=None, securityKey=None, project_attributes = None):
        self.url = url  # source file name
        self.username = projectID
        self.password = securityKey
        self.project_attributes = project_attributes
        super().__init__()
        coloredlogs.install(logger=self.log)

        self.log.info("----------  Initializing BuildingConnectedSpider ---------------")
        self.log.info("URL = {}".format(url))
        self.log.info("User Name  = {}".format(projectID))
        self.log.info("Password  = {}".format(securityKey))
        self.create_project_params['sourceSystem'] = self.name

    def start_requests(self):
        self.log.info("Start Scraping...")
        url = "https://app.buildingconnected.com/api/sso/status/login"

        self.log.info("Login the website...{}" .format(url))

        form_data = {
            "email": self.username
        }
        self.log.info("Input the email...")
        yield scrapy.http.FormRequest(url=url, method='GET', formdata=form_data, callback=self.parse)

    def parse(self , response):
        self.log.info("Input the password...")
        url = "https://app.buildingconnected.com/api/sessions"
        payload = {"username":"jhall@acmecontracting.net","password":"a123456","grant_type":"password"}
        yield scrapy.FormRequest(url = url , method="POST" , formdata=payload ,callback=self.parse_password )

    def parse_password(self , response):
       self.log.info("Logged in the website successfully....")
       keystrings = re.findall(r"\/([^\/]+)$" , self.url)
       if len(keystrings) ==0:
           self.create_project_params['status'] = "Invalid the URL"
           return

       base64filekey = base64.b64decode(keystrings[0])
       decoded_string = base64filekey.decode("utf-8")
       invitionids = re.findall(r"rfps\/([^\/]+)\/" , decoded_string)
       if len(invitionids)==0:
           self.create_project_params['status'] = "Invalid the URL"
           return

       invitation_id  = invitionids[0]
       self.log.info("Get the project page....")
       url = "https://app.buildingconnected.com/api/opportunities/"+invitation_id

       yield scrapy.http.Request(url=url,  callback=self.parse_project)

    def parse_project(self , response):
        self.log.info("Getting the  data  from web page....")
        jsonData  = json.loads(response.body_as_unicode())
        self.create_project_params["project_number"] = jsonData['projectId']
        self.create_project_params["project_name"] = jsonData['name']

        try:
            self.create_project_params["project_address1"] = jsonData['location']['streetNumber']+" "+jsonData['location']['streetName']
        except:
            try:
                self.create_project_params["project_address1"] = jsonData['location']['streetName']
            except:
                self.create_project_params["project_address1"] = ''
                self.create_project_params["project_city"] = jsonData['location']['city']

        self.create_project_params["project_state"] =jsonData['location']['state']

        try:
            self.create_project_params["project_zip"] = jsonData['location']['zip']
        except:
            self.create_project_params["project_zip"] = ''
        self.create_project_params["project_bid_datetime"] =jsonData['dateDue']

        try:
            self.create_project_params["project_desc"] = jsonData['description']
        except:
            self.create_project_params["project_desc"] =  ''

        self.create_project_params['project_files'] = []
        self.create_project_params["project_admin_user_id"] = "Admin User ID From Spider!"
        self.create_project_params['status'] = "Success"
        self.log.info("parse_project created parameters for create_project = {}" .format(self.create_project_params))

        yield scrapy.http.Request(url=response.url+"/files", callback=self.parse_files)

    def parse_files(self , response):
        self.log.info("Getting the file data  from web page....")
        data  = json.loads(response.body_as_unicode())['items']
        folders = []
        files = []
        for item in data:
          if   item['type'] == "FOLDER":
              folders.append(item)
        for item in data:
            if item['type'] =="FILE":
                status = False
                for item2 in folders:
                    if item['scope']['parentId'] == item2['_id']:
                        status = True
                        files.append({'file':item['name'] , 'folder': item2['name'] , 'file_location':item['downloadUrl']})
                if status == False:
                    files.append({'file': item['name'], 'folder':'', 'file_location': item['downloadUrl']})
        for item in files:
            if item['folder'] == '':
                folder_link = "{name}/{projectid}".format(
                    name=self.name,
                    projectid=  self.create_project_params["project_number"])
            else:
                folder_link = "{name}/{projectid}/{folder_name}".format(
                    name=self.name,
                    projectid=  self.create_project_params["project_number"],
                    folder_name=item['folder'])
            yield scrapy.Request(url='https://app.buildingconnected.com' + item['file_location'], callback=self.download_file, meta={'file_name': item['file'],
                                                                                           'folder_name':folder_link})


# --------------  Process Struxture Plans Projects -----------------------------

class StruxturePlansSpider(BaseSpider):
    name = 'struxtureplans'
    url = "https://u5992612.ct.sendgrid.net/wf/click?upn=-2FCtq5rbRgaWbydvLqsY-2FmA0mDswAhVpH6kj-2B49iOtTbrp9zBdlIm5YYeTWWVb7bV5bN9254bcQDx2w6I1Iuu2aJe4tIF8k2oo0MXql4ADTY-3D_1OpY3RcHO6dRZln5p1Fm1ef2pFNgDNe2VBc02FW17vTsNLLGDbIQ8VtAeLDjdk7-2BlMVATvE7fknAVPncy6t-2FGI-2FKjBnBi6ZtJa1q5zV7Y9vmgqnrf0IHPy1R49dA8NLySBbB7imsFS-2B5-2BrNO-2Fxj7o3030ICOW-2FeHIRJTfulFD5mHhfatXTEnA4TvSe9xJj9cKS8I4qfe2jG8h84x63FPH6j2Y-2FZXhp5vDI7sK7Qoc6lZDE8VmOhCijkUN8OJ676-2BmXQBgRowc-2B4QESC5VaWiGT0kEdNSWwd-2FWbd5abuLdXQK8l-2BDEsCXyucoJcQgjuEc-2BRDYgd2gQZemjrIgJ7NV5abTJYvBvvYpx6xmG-2FoLHCOcZN8Ow-2BSuO3wevsDXVGsYoEdtyD98-2Fzk0dCpg75Oo1A-3D-3D"
    log = logging.getLogger(name)

    def __init__(self, url=None, projectID=None, securityKey=None, project_attributes = None):
        self.url = url  # source file name
        self.project_attributes = project_attributes
        super().__init__()
        coloredlogs.install(logger=self.log)
        self.log.info("URL = {}".format(url))
        self.create_project_params['sourceSystem'] = self.name

    def start_requests(self):
        self.log.info("Start Scraping...")
        self.log.info("Login the website...")
        yield scrapy.Request(url=self.url, callback=self.parse)

    def parse(self , response):
        self.log.info("Logged in the website successfully....")
        self.log.info("Getting the  data  from web page....")
        project_ids = re.findall(r'jobs\/([0-9]+)\/details', response.url)
        if len(project_ids)==0:
            self.create_project_params['status'] = "Invalid Url!"
            return
        self.create_project_params["project_number"] = project_ids[0]
        self.create_project_params["project_name"] = self.clean_text(response.xpath('//h1[@class="project-name"]/text()').extract_first())
        self.create_project_params["project_address1"] = ''
        locations =  response.xpath('//h3[text()="Location"]/following-sibling::p/text()').extract_first()
        if len(locations.split(','))>1:
            self.create_project_params["project_city"] =locations.split(',')[0]
            if len(locations.split(',')[1].split(' '))>1:
                self.create_project_params["project_state"] = locations.split(',')[1].strip().split(' ')[0]
                self.create_project_params["project_zip"] = locations.split(',')[1].strip().split(' ')[1]
            else:
                self.create_project_params["project_state"] = ''
                self.create_project_params["project_zip"] = ''
        else:
            self.create_project_params["project_city"]= ''
            self.create_project_params["project_bid_datetime"] =  response.xpath('//strong[text()="Bid Date"]/following-sibling::text()[1]').extract_first()
        self.create_project_params["project_desc"] =' '.join(response.xpath('//div[@class="item notes"]/div[@class="details"]/p').extract())
        self.create_project_params['project_files'] = []
        self.create_project_params["project_admin_user_id"] = "Admin User ID From Spider!"
        self.create_project_params['status'] = "Success"
        self.log.info("Getting the file data  from web page....")
        for  file in response.xpath('//h3[text()="Documents"]/following-sibling::div/a'):
            file_link  = file.xpath('./@href').extract_first()
            file_name  = file.xpath('./text()').extract_first()
            yield scrapy.Request(url='https://www.struxtureplans.com'+file_link.replace('/document/', '/document/source/'), callback=self.download_file , meta={'file_name':file_name , 'folder_name':"{name}/{projectid}/{folder_name}".format(name = self.name , projectid =self.create_project_params["project_number"] , folder_name = "Documents" )})




def br_scrape(soucesystem , url, username, password, project_attributes):
    logger.info("Selecting the sourceSystem......")
    # this routine is called from the 920 lambda routine, or other external applications and passes the information necessary to scrape the site.
    try:

        process = CrawlerProcess({'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'})
        if soucesystem == "pipelinesuite":
            process.crawl(PipelineSuiteSpider , url = url, projectID = username, securityKey = password, project_attributes = project_attributes)
        elif soucesystem == "gradebeam":
            process.crawl(GradebeamSpider ,  url = url, projectID = None, securityKey = None, project_attributes = project_attributes)
        elif soucesystem == "smartbid":
            process.crawl(SmartbidSpider ,  url = url, projectID = None, securityKey = None, project_attributes = project_attributes)
        elif soucesystem == "buildingconnected":
            process.crawl(BuildingConnectedSpider ,  url = url, projectID = username, securityKey = password, project_attributes = project_attributes)
        elif soucesystem == "struxtureplans":
            process.crawl(StruxturePlansSpider ,  url = url, projectID = username, securityKey = password, project_attributes = project_attributes)

        process.start()

    except Exception as e:
        logger.error("The br_scrape routine failed with error = {} " .format(e))

if __name__ == "__main__":

    try:


        project_id = "test1projectid"
        submission_id = "test1submission_id"
        submitter_email = "doug@dougbower.com"
        user_timezone = "eastern"
        submission_datetime = "01-01-2018"

        project_attributes={}
        project_attributes["project_id"] = project_id
        project_attributes["submission_id"] = submission_id
        project_attributes["submitter_email"] = submitter_email
        project_attributes["user_timezone"] = user_timezone
        project_attributes["submission_datetime"] = submission_datetime

        # logger.info("About to scrape gradebeam")
        # url = "https://www1.gradebeam.com/bidresponse/e2l0YmlkOjEzNDI2NjAzMyxvcmdpZDo5NzA5NDUsaXRicmVzcG9uc2U6MCx1c2VyaWQ6MH0="
        # br_scrape("gradebeam", url, "", "")
        logger.info("About to scrape pipeline")
        # url = "https://fortneyweygandt.pipelinesuite.com/ehPipelineSubs/dspProject/projectID/118431"
        # projectID = "94505"
        # securityKey = "G3qNxcCb9"
        # logger.info("Checking the Url and Data...")
        # br_scrape("pipelinesuite", url, projectID, securityKey)
        logger.info("About to scrape gradebeam")
        url = "https://secure.smartbidnet.com/Main/Login.aspx?cId=bp_587275071&sPassportKey=0D415A5C1527C8F32965D7B34AB9424A250AB275&sBidId=393182&e=1"
        br_scrape("smartbid", url, "", "", project_attributes)
        # logger.info("About to scrape buildingconnected")
        # url = "https://app.buildingconnected.com/_/action/L18vcmZwcy81YmE2YmI4NTUyOGI3NzAwMzIyNGM0YTgvMmM1ZjRkZTgtMzk4Ny00OTdiLWI2YjAtMThiZTE0YzE4NTM2P3NoYXJlPTAmaW52aXRlcklkPTU5MWM1Zjc4NDM3MTllMTAwMDI0YjQ5ZiZpbnZpdGVlSWQ9NWJhNmJiODU1MjhiNzcwMDMyMjRjNGEz"
        # username = "jhall@acmecontracting.net"
        # password = "a123456"
        # logger.info("Checking the Url and Data...")
        # br_scrape("buildingconnected", url, username, password)
        # logger.info("About to scrape struxtureplans")
        # url = "https://u5992612.ct.sendgrid.net/wf/click?upn=-2FCtq5rbRgaWbydvLqsY-2FmA0mDswAhVpH6kj-2B49iOtTbrp9zBdlIm5YYeTWWVb7bV5bN9254bcQDx2w6I1Iuu2aJe4tIF8k2oo0MXql4ADTY-3D_1OpY3RcHO6dRZln5p1Fm1ef2pFNgDNe2VBc02FW17vTsNLLGDbIQ8VtAeLDjdk7-2BlMVATvE7fknAVPncy6t-2FGI-2FKjBnBi6ZtJa1q5zV7Y9vmgqnrf0IHPy1R49dA8NLySBbB7imsFS-2B5-2BrNO-2Fxj7o3030ICOW-2FeHIRJTfulFD5mHhfatXTEnA4TvSe9xJj9cKS8I4qfe2jG8h84x63FPH6j2Y-2FZXhp5vDI7sK7Qoc6lZDE8VmOhCijkUN8OJ676-2BmXQBgRowc-2B4QESC5VaWiGT0kEdNSWwd-2FWbd5abuLdXQK8l-2BDEsCXyucoJcQgjuEc-2BRDYgd2gQZemjrIgJ7NV5abTJYvBvvYpx6xmG-2FoLHCOcZN8Ow-2BSuO3wevsDXVGsYoEdtyD98-2Fzk0dCpg75Oo1A-3D-3D"
        # logger.info("Checking the Url and Data...")
        # br_scrape("struxtureplans", url, "", "")


    except Exception as e:
        logger.error(" error = {}".format(e))


