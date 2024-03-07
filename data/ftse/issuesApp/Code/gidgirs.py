import sqlite3
import json
import requests
from bs4 import BeautifulSoup
import os
from newsapi import NewsApiClient
import openai
from datetime import datetime, timedelta

#resolve date range
def resolve_daterange(daterange):
    if daterange in ["past day", "past week", "past month", "past year"]:
        end_date = datetime.now()
        if daterange =="past day":
            start_date = end_date - timedelta(days=1)            
        if daterange =="past week":
            start_date = end_date - timedelta(days=7)
        if daterange =="past month":
            start_date = end_date - timedelta(days=30)
        if daterange =="past year":
            start_date = end_date - timedelta(days=365)
        return(start_date.strftime('%Y-%m-%d'),end_date.strftime('%Y-%m-%d'))
    else:
        if  'start_date' in daterange:
            x1=datetime.strptime( daterange["start_date"], '%d %b %Y').date() 
            if  'end_date' in daterange:
                x2=datetime.strptime( daterange["end_date"], '%d %b %Y').date()
            return x1,x2
        else:
            print("Invalid date range - expecting 'past day', 'past week', 'past month' or 'past year'")



def getContent(url):
    
    try:
        # Make a GET request to the URL
        response = requests.get(url)

        # Check if the request was successful
        if response.status_code == 200:            
            # Extract clean text from the parsed HTML            
            soup = BeautifulSoup(response.content, 'html.parser')
            clean_text = soup.get_text(separator=' ', strip=True)

            return clean_text
        else:
            return (f"Failed to fetch the web content. Status code: {response.status_code}")
        
    except:
           return (f"Failed to fetch the web content.")
    

def getContentFromRawData(ID):
    conn = sqlite3.connect('gidgirs_db')
    cur = conn.cursor()
    # SQL statement to select all data from the RawData table
    select_sql = 'SELECT content FROM RawData WHERE ID='+str(ID)

    # Execute the SQL statement
    cur.execute(select_sql)

    # Fetch all rows from the query result
    rows = cur.fetchall()

    # Iterate over the rows to access the retrieved data
    contentFromRaw=""
    for row in rows:
        json_data = row[0]
        contentFromRaw=json_data

    # Close the database connection
    conn.close()
    return contentFromRaw


def getUrlFromRawData(ID):
    conn = sqlite3.connect('gidgirs_db')
    cur = conn.cursor()
    # SQL statement to select all data from the RawData table
    select_sql = 'SELECT URL FROM RawData WHERE ID='+str(ID)

    # Execute the SQL statement
    cur.execute(select_sql)

    # Fetch all rows from the query result
    rows = cur.fetchall()

    # Iterate over the rows to access the retrieved data
    contentFromRaw=""
    for row in rows:
        json_data = row[0]
        contentFromRaw=json_data

    # Close the database connection
    conn.close()
    return contentFromRaw


def commitArticle(article,search_criteria):
    # Connect to the db_dgirs database
    conn = sqlite3.connect('gidgirs_db')

    # Create a cursor object
    cur = conn.cursor()

    # SQL statement to insert new data into the RawData table
    insert_sql = """INSERT INTO RawData 
                    (JSON_Data,search_criteria,title,author,source,publishedAt,url,content) 
                    VALUES (?,?,?,?,?,?,?,?)"""
    # Execute the SQL statement with the JSON data
    cur.execute(insert_sql, 
                (str(article),
                str(search_criteria),
                str(article["title"]),
                str(article["author"]),
                str(article["source"]),
                str(article["publishedAt"]),
                str(article["url"]),
                ""
                ),)
    
    # Commit the changes to the database
    conn.commit()
    lastrowid= cur.lastrowid
    # Close the database connection
    conn.close()
    return lastrowid

def updateContent(articleID):
    #be sure that there is no content already
    content=getContentFromRawData(articleID)
    if content != "":
        return 0

    #Get the url from the table
    url = getUrlFromRawData(articleID)

    #update the table with content
    # Connect to the db_dgirs database
    conn = sqlite3.connect('gidgirs_db')

    # Create a cursor object
    cur = conn.cursor()

    # SQL statement to insert new data into the RawData table
    update_sql = """UPDATE RawData SET content=? WHERE ID=?"""
    # Execute the SQL statement with the JSON data
    cur.execute(update_sql, 
                (getContent(url),
                str(articleID),
                ),)
    
    # Commit the changes to the database
    conn.commit()
    # Close the database connection
    conn.close()
    return articleID

def updateCountryDomain(articleID, country,domain):
    #be sure that there is no content already

    # Connect to the db_dgirs database
    conn = sqlite3.connect('gidgirs_db')

    # Create a cursor object
    cur = conn.cursor()

    # SQL statement to insert new data into the RawData table
    update_sql = """UPDATE RawData SET country=?,domain=? WHERE ID=?"""
    # Execute the SQL statement with the JSON data
    cur.execute(update_sql, 
                (country,
                 domain,
                str(articleID),
                ),)
    
    # Commit the changes to the database
    conn.commit()
    # Close the database connection
    conn.close()
    return articleID

def updateSummary(articleID):
    #be sure that there is no content already
    content = getContentFromRawData(articleID)
    _summary = ""
    if len(content) < 250:
        return articleID
    
    #get the summary of the content
    try:
        response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                {
                "role": "system", 
                "content": 
                """
                Give a summary of the given text in no more than 250 words

                Give the result always in JSON form with the following example
                {"Summary":"summary of content"}
                """
                },
                {"role": "user", "content": content}
                ]
            )
        _summary = json.loads(response.choices[0].message["content"])
        
        _summary=_summary["Summary"]
    except Exception as e:
        return "error-"+str(e)
    #print(_summary)
    #summary=_summary["Summary"]

    # Connect to the db_dgirs database
    conn = sqlite3.connect('gidgirs_db')

    # Create a cursor object
    cur = conn.cursor()

    # SQL statement to insert new data into the RawData table
    update_sql = """UPDATE RawData SET Summary=? WHERE ID=?"""
    # Execute the SQL statement with the JSON data
    cur.execute(update_sql, 
                (_summary,
                str(articleID),
                ),)
    
    # Commit the changes to the database
    conn.commit()
    # Close the database connection
    conn.close()
    return articleID

def update_missing_contents(articleList):
    #find IDs of articles in the RawJSOMData table
    conn = sqlite3.connect('gidgirs_db')
    cur = conn.cursor()
    # SQL statement to select all data from the RawData table
    alist=", ".join(list(map(lambda item: str(item) ,  articleList)))
    select_sql = 'SELECT ID FROM RawData WHERE ID IN ('+alist+') AND content=""'


    # Execute the SQL statement
    cur.execute(select_sql)

    # Fetch all rows from the query result
    rows = cur.fetchall()

    # Iterate over the rows to access the retrieved data
    for row in rows:
        ID = row[0]
        updateContent(ID)

    # Close the database connection
    conn.close()
    return rows    

def gatherRawContentForCountries(search_text, daterange):
    
    start_date,end_date=resolve_daterange(daterange)
    search_text=search_text
    search_criteria={"q": search_text,#country + ' ' + search_text, 
                 "from_param":start_date,  #'2024-01-01', 
                 "to":end_date,#'2024-02-27',
                 "language":'en',
                 "page":1}
    print(search_criteria)
    if SearchAlreadyExistsForArticle(search_criteria):
        return []
    

    newsapi = NewsApiClient(api_key=os.getenv("NEWS_API_KEY"))    


    newpage=True
    pageCount=0
    all_articles_batches=[]
    while newpage==True:
        _all_articles_batch = newsapi.get_everything(q=search_criteria["q"],
                                            #sources='bbc-news,the-verge',
                                            #domains='bbc.co.uk,techcrunch.com',
                                            from_param=search_criteria["from_param"],
                                            to=search_criteria["to"],
                                            language=search_criteria["language"],
                                            sort_by="relevancy",
                                            exclude_domains="vietnamplus.vn,marketscreener.com,thegatewaypundit.com,rt.com,androidpolice.com,makeuseof.com",
                                            page=search_criteria["page"])
    
        all_articles_batches.append(_all_articles_batch)
        totalResults = _all_articles_batch["totalResults"]
        
        pageCount=pageCount+1
        print("Retrieving data from the web - Page "+str(pageCount) +" of data comprising of " + str(totalResults) + " articles" )
        if pageCount * 100 > totalResults:
            newpage = False

    
    committedArticleIDs=[]
    batchCount = 1
    for articleBatch in all_articles_batches:
        print("Committing to DB articles batch - "+ str(batchCount))
        batchCount=batchCount+1
        for article in articleBatch['articles']:
            committedArticleID= commitArticle(article,search_criteria)
            committedArticleIDs.append(committedArticleID)          
    
    
    print("list of article IDs - " , committedArticleIDs)
    return committedArticleIDs


def llmDetermineSubjectCountryDomain(inputcontent):
    if  inputcontent=="":
        return None
    
    if  len(inputcontent)<250 :
        return None
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
            {
            "role": "system", 
            "content": 
                        """
            Identify which country the content is mainly about.

            Also classify the content based on one of the following domains: "Geo-economics", "Political", "Environmental", "Social", "Technology" and  "Other"  
 
            Give the result always in JSON form with the following example
            {"Country":"Ghana","Domain":"Technology"}
            """
            },
            {"role": "user", "content": inputcontent}
]
)
        content = response.choices[0].message["content"]
        return content
    except Exception as e:
        return "error"

    

def update_missing_countries_and_domains(articleList):
    #find IDs of articles in the RawJSOMData table
    conn = sqlite3.connect('gidgirs_db')
    cur = conn.cursor()

    alist=", ".join(list(map(lambda item: str(item) ,  articleList)))

    # SQL statement to select all data from the RawData table
    select_sql = "SELECT ID FROM RawData WHERE ID IN ("+alist+") AND country IS NULL OR trim(country) = ''"

    # Execute the SQL statement
    cur.execute(select_sql)

    # Fetch all rows from the query result
    rows = cur.fetchall()

    # Iterate over the rows to access the retrieved data
    for row in rows:
        ID = row[0]
        content=getContentFromRawData(ID)
        countrydomain=llmDetermineSubjectCountryDomain(content)
        if countrydomain ==None:
            pass
        else:
            try:
                country_obj = json.loads(countrydomain)
                updateCountryDomain(ID,country_obj["Country"],country_obj["Domain"])
            except:
                updateCountryDomain(ID,"N/A","N/A")

    # Close the database connection
    conn.close()
    return rows    

def update_missing_summaries(articleList):
    #find IDs of articles in the RawJSOMData table
    conn = sqlite3.connect('gidgirs_db')
    cur = conn.cursor()
    # SQL statement to select all data from the RawData table
    alist=", ".join(list(map(lambda item: str(item) ,  articleList)))

    select_sql = "SELECT ID FROM RawData WHERE ID IN ("+alist+") AND Summary IS NULL OR trim(country) = ''"

    # Execute the SQL statement
    cur.execute(select_sql)

    # Fetch all rows from the query result
    rows = cur.fetchall()

    # Iterate over the rows to access the retrieved data
    for row in rows:
        ID = row[0]
        updateSummary(ID)
  
    # Close the database connection
    conn.close()
    return rows  

def llmAnalise_OLD(inputcontent):

    if  inputcontent=="":
        return None
    
    if  len(inputcontent)<250 :
        return None

    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system", 
                    "content": 
                                """
            You are a content analyst from the Public and Regulatory Affairs department of a financial organisation who can extract specific geoeconomic information relating to 
            country-specific issues in geo-economic, political, environmental, social, and technology domains, from article text. 

            Your organization operates across multiple segments of the financial services industry, offering a broad spectrum of banking and financial services to various 
            customer segments, including individuals, businesses, and corporations. It also has global presence and focus on emerging markets. 

            Extract the following information in relation to the country that is the main subject of the article - 

            In terms of Regulatory Changes:
                1.Title of the regulatory change.
                2.Detailed description of the regulatory change.
                3.The impact of the regulatory change

            In terms of FinTech Innovations:
                1.Title of the FinTech innovation.
                2.Detailed description of the innovation.
                3.Launch date of the innovation.
                4.Current regulatory status of the innovation.
                5.The impact of the FinTech innovation.

            In terms of Cybersecurity Threats:
                1.Name of the cybersecurity incident.
                2.Detailed description of the threat.
                3.Date of the cybersecurity incident.
                4.Entities affected by the threat.
                5.Measures taken to mitigate the threat.
                6.The impact of the cybersecurity threat.

            In terms of Digital Transformation Trends:
                1.Title of the digital transformation trend.
                2.Detailed description of the trend.
                3.Relevance of the trend.
                4.Current adoption status of the trend.
                5.The impact of the digital transformation trend.

            In terms of Consumer Protection:
                1.Title of the consumer protection issue.
                2.Detailed description of the issue.
                3.Legislation related to the issue.
                4.Date related to the issue or legislation.
                5.Measures taken for consumer protection.
                6.The impact of the issue on consumer protection.

            In terms of Market Dynamics:
                1.Title of the market event.
                2.Detailed description of the event.
                3.Date of the event.
                4.The impact of the event on the market.
                5.The response to the market event.
                6.The date on which observations were made or reported.

            In terms of Sustainability in Technology:
                1.Title of the sustainability initiative.
                2.Detailed description of the initiative.
                3.Start date of the initiative.
                4.End date of the initiative, if applicable.
                5.The impact of the sustainability initiative.
                6.The outcomes of the initiative.

            In terms of Economic Policies:
                1.Title of the economic policy.
                2.Detailed description of the policy.
                3.The impact of the economic policy.
                4.Date the policy was announced.

            In terms of Political Events:
                1.Title of the political event.
                2.Detailed description of the event.
                3.The impact of the political event.
                4.Date of the political event.

            In terms of Social Movements:
                1.Title of the social movement.
                2.Detailed description of the social movement.
                3.The impact of the social movement.
                4.Start date of the social movement.

            In terms of Environmental Issues:
                1.Title of the environmental issue.
                2.Detailed description of the issue.
                3.The impact of the environmental issue.
                4.Date the issue was observed or reported.

            In terms of Technological Innovations:
                1.Title of the technological innovation.
                2.Detailed description of the innovation.
                3.The impact of the technological innovation.
                4.Date the innovation was announced

            In terms of Overall Organisation Consideration:
                1.Justification of relevance to the organization
                2.Relevance and importance percentage
                3.Rationale for assigning the percentage
                4.A very brief title of the issue e.g "Brexit and Trade Relations", "Regulatory Changes and Financial Services","Climate Change and Sustainable Finance","Digital Transformation and Fintech Innovation","Geopolitical Developments and Trade Policies"
                5.A very brief summary (80 words) of the issue 

            Where any of these enquiries are not available, give N/A for their result

            Give the result always in JSON form with the following example
           {
            "RegulatoryChangeTitle":"extracted information for RegulatoryChangeTitle",
            "RegulatoryChangeDescription":"extracted information for RegulatoryChangeDescription",
            "RegulatoryChangeImpact":"extracted information for RegulatoryChangeImpact",
            "FinTechInnovationTitle":"extracted information for FinTechInnovationTitle",
            "FinTechInnovationDescription":"extracted information for FinTechInnovationDescription",
            "FinTechInnovationLaunchDate":"extracted information for FinTechInnovationLaunchDate",
            "FinTechInnovationRegulatoryStatus":"extracted information for FinTechInnovationRegulatoryStatus",
            "FinTechInnovationImpact":"extracted information for FinTechInnovationImpact",
            "CybersecurityThreatIncidentName":"extracted information for CybersecurityThreatIncidentName",
            "CybersecurityThreatDescription":"extracted information for CybersecurityThreatDescription",
            "CybersecurityThreatDate":"extracted information for CybersecurityThreatDate",
            "CybersecurityThreatAffectedEntities":"extracted information for CybersecurityThreatAffectedEntities",
            "CybersecurityThreatMitigationMeasures":"extracted information for CybersecurityThreatMitigationMeasures",
            "CybersecurityThreatImpact":"extracted information for CybersecurityThreatImpact",
            "DigitalTransformationTrendTitle":"extracted information for DigitalTransformationTrendTitle",
            "DigitalTransformationDescription":"extracted information for DigitalTransformationDescription",
            "DigitalTransformationRelevance":"extracted information for DigitalTransformationRelevance",
            "DigitalTransformationAdoptionStatus":"extracted information for DigitalTransformationAdoptionStatus",
            "DigitalTransformationImpact":"extracted information for DigitalTransformationImpact",
            "ConsumerProtectionIssueTitle":"extracted information for ConsumerProtectionIssueTitle",
            "ConsumerProtectionDescription":"extracted information for ConsumerProtectionDescription",
            "ConsumerProtectionLegislation":"extracted information for ConsumerProtectionLegislation",
            "ConsumerProtectionDate":"extracted information for ConsumerProtectionDate",
            "ConsumerProtectionMeasures":"extracted information for ConsumerProtectionMeasures",
            "ConsumerProtectionImpact":"extracted information for ConsumerProtectionImpact",
            "MarketDynamicsEventTitle":"extracted information for MarketDynamicsEventTitle",
            "MarketDynamicsDescription":"extracted information for MarketDynamicsDescription",
            "MarketDynamicsDate":"extracted information for MarketDynamicsDate",
            "MarketDynamicsImpact":"extracted information for MarketDynamicsImpact",
            "MarketDynamicsResponse":"extracted information for MarketDynamicsResponse",
            "MarketDynamicsObservationDate":"extracted information for MarketDynamicsObservationDate",
            "SustainabilityInTechInitiativeTitle":"extracted information for SustainabilityInTechInitiativeTitle",
            "SustainabilityInTechDescription":"extracted information for SustainabilityInTechDescription",
            "SustainabilityInTechStartDate":"extracted information for SustainabilityInTechStartDate",
            "SustainabilityInTechEndDate":"extracted information for SustainabilityInTechEndDate",
            "SustainabilityInTechImpact":"extracted information for SustainabilityInTechImpact",
            "SustainabilityInTechOutcomes":"extracted information for SustainabilityInTechOutcomes",
            "EconomicPolicyTitle":"extracted information for EconomicPolicyTitle",
            "EconomicPolicyDescription":"extracted information for EconomicPolicyDescription",
            "EconomicPolicyImpact":"extracted information for EconomicPolicyImpact",
            "EconomicPolicyAnnouncedDate":"extracted information for EconomicPolicyAnnouncedDate",
            "PoliticalEventTitle":"extracted information for PoliticalEventTitle",
            "PoliticalEventDescription":"extracted information for PoliticalEventDescription",
            "PoliticalEventImpact":"extracted information for PoliticalEventImpact",
            "PoliticalEventDate":"extracted information for PoliticalEventDate",
            "SocialMovementTitle":"extracted information for SocialMovementTitle",
            "SocialMovementDescription":"extracted information for SocialMovementDescription",
            "SocialMovementImpact":"extracted information for SocialMovementImpact",
            "SocialMovementStartDate":"extracted information for SocialMovementStartDate",
            "EnvironmentalIssueTitle":"extracted information for EnvironmentalIssueTitle",
            "EnvironmentalIssueDescription":"extracted information for EnvironmentalIssueDescription",
            "EnvironmentalIssueImpact":"extracted information for EnvironmentalIssueImpact",
            "EnvironmentalIssueObservationDate":"extracted information for EnvironmentalIssueObservationDate",
            "TechnologicalInnovationTitle":"extracted information for TechnologicalInnovationTitle",
            "TechnologicalInnovationDescription":"extracted information for TechnologicalInnovationDescription",
            "TechnologicalInnovationImpact":"extracted information for TechnologicalInnovationImpact",
            "TechnologicalInnovationAnnouncementDate":"extracted information for TechnologicalInnovationAnnouncementDate",
            "OverallOrganisationConsiderationJustification":"extracted information for OverallOrganisationConsiderationJustification",
            "OverallOrganisationConsiderationRelevanceAndImportance":"extracted information for OverallOrganisationConsiderationRelevanceAndImportance",
            "OverallOrganisationConsiderationRationale":"extracted information for OverallOrganisationConsiderationRationale".
            "OverAllOrganisationVeryBriefTitleOftheIssue":"A very brief title of the issue",
            "OverAllOrganisationVeryBriefSummaryOftheIssue":"A very brief summary of the issue"
            }
"""
    },
    {"role": "user", "content": inputcontent}
]
)
        content = response.choices[0].message["content"]
        return content
    except Exception as e:
        print(e)
        return "error-"+str(e)

def llmAnalise(inputcontent):

    if  inputcontent=="":
        return None
    
    if  len(inputcontent)<250 :
        return None

    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system", 
                    "content": 
                                """
            You are a content analyst from the Public and Regulatory Affairs department of a financial organisation who can extract specific geoeconomic information relating to 
            country-specific issues in geo-economic, political, environmental, social, and technology domains, from article text. 

            Your organization operates across multiple segments of the financial services industry, offering a broad spectrum of banking and financial services to various 
            customer segments, including individuals, businesses, and corporations. It also has global presence and focus on emerging markets. 

            Extract the following information in relation to the country that is the main subject of the article - 


            In terms of Overall Organisation Consideration:
                1.Justification of relevance to the organization
                2.Relevance and importance percentage (without any comment)
                3.Rationale for assigning the percentage
                4.A very brief title of the issue e.g "Brexit and Trade Relations", "Regulatory Changes and Financial Services","Climate Change and Sustainable Finance","Digital Transformation and Fintech Innovation","Geopolitical Developments and Trade Policies"
                5.A very brief summary (80 words) of the issue 

            Where any of these enquiries are not available, give N/A for their result

            Give the result always in JSON form with the following example
           {
            "RegulatoryChangeTitle":"extracted information for RegulatoryChangeTitle",
            "RegulatoryChangeDescription":"extracted information for RegulatoryChangeDescription",
            "RegulatoryChangeImpact":"extracted information for RegulatoryChangeImpact",
            "FinTechInnovationTitle":"extracted information for FinTechInnovationTitle",
            "FinTechInnovationDescription":"extracted information for FinTechInnovationDescription",
            "FinTechInnovationLaunchDate":"extracted information for FinTechInnovationLaunchDate",
            "FinTechInnovationRegulatoryStatus":"extracted information for FinTechInnovationRegulatoryStatus",
            "FinTechInnovationImpact":"extracted information for FinTechInnovationImpact",
            "CybersecurityThreatIncidentName":"extracted information for CybersecurityThreatIncidentName",
            "CybersecurityThreatDescription":"extracted information for CybersecurityThreatDescription",
            "CybersecurityThreatDate":"extracted information for CybersecurityThreatDate",
            "CybersecurityThreatAffectedEntities":"extracted information for CybersecurityThreatAffectedEntities",
            "CybersecurityThreatMitigationMeasures":"extracted information for CybersecurityThreatMitigationMeasures",
            "CybersecurityThreatImpact":"extracted information for CybersecurityThreatImpact",
            "DigitalTransformationTrendTitle":"extracted information for DigitalTransformationTrendTitle",
            "DigitalTransformationDescription":"extracted information for DigitalTransformationDescription",
            "DigitalTransformationRelevance":"extracted information for DigitalTransformationRelevance",
            "DigitalTransformationAdoptionStatus":"extracted information for DigitalTransformationAdoptionStatus",
            "DigitalTransformationImpact":"extracted information for DigitalTransformationImpact",
            "ConsumerProtectionIssueTitle":"extracted information for ConsumerProtectionIssueTitle",
            "ConsumerProtectionDescription":"extracted information for ConsumerProtectionDescription",
            "ConsumerProtectionLegislation":"extracted information for ConsumerProtectionLegislation",
            "ConsumerProtectionDate":"extracted information for ConsumerProtectionDate",
            "ConsumerProtectionMeasures":"extracted information for ConsumerProtectionMeasures",
            "ConsumerProtectionImpact":"extracted information for ConsumerProtectionImpact",
            "MarketDynamicsEventTitle":"extracted information for MarketDynamicsEventTitle",
            "MarketDynamicsDescription":"extracted information for MarketDynamicsDescription",
            "MarketDynamicsDate":"extracted information for MarketDynamicsDate",
            "MarketDynamicsImpact":"extracted information for MarketDynamicsImpact",
            "MarketDynamicsResponse":"extracted information for MarketDynamicsResponse",
            "MarketDynamicsObservationDate":"extracted information for MarketDynamicsObservationDate",
            "SustainabilityInTechInitiativeTitle":"extracted information for SustainabilityInTechInitiativeTitle",
            "SustainabilityInTechDescription":"extracted information for SustainabilityInTechDescription",
            "SustainabilityInTechStartDate":"extracted information for SustainabilityInTechStartDate",
            "SustainabilityInTechEndDate":"extracted information for SustainabilityInTechEndDate",
            "SustainabilityInTechImpact":"extracted information for SustainabilityInTechImpact",
            "SustainabilityInTechOutcomes":"extracted information for SustainabilityInTechOutcomes",
            "EconomicPolicyTitle":"extracted information for EconomicPolicyTitle",
            "EconomicPolicyDescription":"extracted information for EconomicPolicyDescription",
            "EconomicPolicyImpact":"extracted information for EconomicPolicyImpact",
            "EconomicPolicyAnnouncedDate":"extracted information for EconomicPolicyAnnouncedDate",
            "PoliticalEventTitle":"extracted information for PoliticalEventTitle",
            "PoliticalEventDescription":"extracted information for PoliticalEventDescription",
            "PoliticalEventImpact":"extracted information for PoliticalEventImpact",
            "PoliticalEventDate":"extracted information for PoliticalEventDate",
            "SocialMovementTitle":"extracted information for SocialMovementTitle",
            "SocialMovementDescription":"extracted information for SocialMovementDescription",
            "SocialMovementImpact":"extracted information for SocialMovementImpact",
            "SocialMovementStartDate":"extracted information for SocialMovementStartDate",
            "EnvironmentalIssueTitle":"extracted information for EnvironmentalIssueTitle",
            "EnvironmentalIssueDescription":"extracted information for EnvironmentalIssueDescription",
            "EnvironmentalIssueImpact":"extracted information for EnvironmentalIssueImpact",
            "EnvironmentalIssueObservationDate":"extracted information for EnvironmentalIssueObservationDate",
            "TechnologicalInnovationTitle":"extracted information for TechnologicalInnovationTitle",
            "TechnologicalInnovationDescription":"extracted information for TechnologicalInnovationDescription",
            "TechnologicalInnovationImpact":"extracted information for TechnologicalInnovationImpact",
            "TechnologicalInnovationAnnouncementDate":"extracted information for TechnologicalInnovationAnnouncementDate",
            "OverallOrganisationConsiderationJustification":"extracted information for OverallOrganisationConsiderationJustification",
            "OverallOrganisationConsiderationRelevanceAndImportance":"extracted information for OverallOrganisationConsiderationRelevanceAndImportance",
            "OverallOrganisationConsiderationRationale":"extracted information for OverallOrganisationConsiderationRationale".
            "OverAllOrganisationVeryBriefTitleOftheIssue":"A very brief title of the issue",
            "OverAllOrganisationVeryBriefSummaryOftheIssue":"A very brief summary of the issue"
            }
"""
    },
    {"role": "user", "content": inputcontent}
]
)
        content = response.choices[0].message["content"]
        return content
    except Exception as e:
        print(e)
        return "error-"+str(e)


def commitRawAnalysis(ArticleID,JSON_Data):
    # Connect to the db_dgirs database
    conn = sqlite3.connect('gidgirs_db')

    # Create a cursor object
    cur = conn.cursor()

    # SQL statement to insert new data into the RawData table
    insert_sql = """INSERT INTO RawAnalysis
                    (ArticleID, JSON_Data)
                    VALUES(?, ?);
                 """
    # Execute the SQL statement with the JSON data
    cur.execute(insert_sql, 
                (
                str(ArticleID),
                str(JSON_Data),
                ),)
    # Commit the changes to the database
    conn.commit()
    # Close the database connection
    conn.close()


def generateRawAnalysis(ArticleID, country_list):
    conn = sqlite3.connect('gidgirs_db')
    cur = conn.cursor()
    # SQL statement to select all data from the RawData table
    select_sql = 'SELECT JSON_data, content,country FROM RawData WHERE ID='+str(ArticleID)

    # Execute the SQL statement
    cur.execute(select_sql)

    # Fetch all rows from the query result
    rows = cur.fetchall()

    # Iterate over the rows to access the retrieved data
    article_info=None
    outputcontent=None

    if len(rows)==0:
        return None
    for row in rows:
        json_data = row[0]
        content   = row[1]
        country   = row[2]

        if  "Status code: 403" in content:
            return None
        
        if len(content) < 250:
            return None
        
        if country in country_list:
            outputcontent=llmAnalise(content)
            commitRawAnalysis(ArticleID,outputcontent)
            article_info={"country":country,"json_data":json_data,"outputcontent":outputcontent,"content":content}


    # Close the database connection
    conn.close()
    return article_info


#GPRA Issues Data Gathering and Reporting Solution (GIDGRS)
#This is the top level entry poinf function that
#gets articles online, extracts the content and analizes them for GPRA issues
#The results are stored in a database tables RawData and RawAnalysis respectively. 


def GidGrs(daterange, country_list):
    search_text="(Geo economic OR political OR environmental OR social OR technology)"
    savedarticles=gatherRawContentForCountries(search_text, daterange)

    #update Raw data table with Web content using available URL
    print("Updating raw data with content")
    update_missing_contents() 

    #update Raw data table with Web country using content URL
    print("Updating raw data with countries")
    update_missing_countries_and_domains()

    #generate detailed analysis only for requested countryes
    for articleid in savedarticles:
        generateRawAnalysis(articleid, country_list)



#country_list=["United Kingdom","India","Pakistan","United States","Hong Kong","USA","England","Nigeria","Ghana"]
#GidGrs("past week",country_list)
#GidGrs({"start_date":"2 Jan 2022","end_date":"2 Jan 2022"},country_list)
#generateRawAnalysis(4380, country_list)
#update_missing_countries_and_domains()
#updateSummary(4382)
#update_missing_summaries()
#generateRawAnalysis(4512, country_list)

def getArticlesForSearchByCountryAndDate(daterange,country):
    search_text="(("+country+") AND (Geo economic OR political OR environmental OR social OR technology))"
    
    savedarticles=gatherRawContentForCountries(search_text, daterange)
    
    print("Gnerating contents..")
    update_missing_contents(savedarticles) 
    
    print("Appending countries and domains")
    update_missing_countries_and_domains(savedarticles)
    tidy_up_countries(savedarticles)

    print("generating summaries")
    update_missing_summaries(savedarticles)
    return

def reformat_date(date_string):
    # Convert the input string to a datetime object
    date_obj = datetime.strptime(date_string, '%Y-%m-%d')
    
    # Format the datetime object as a string in the desired format
    formatted_date = date_obj.strftime('%d %b %Y')
    
    return formatted_date

def clear_database_table(tableNae):
    conn = sqlite3.connect('gidgirs_db')
    cur = conn.cursor()
    cur.execute("DELETE FROM "+tableNae)
    conn.commit()


def tidy_up_countries(articleList):
    conn = sqlite3.connect('gidgirs_db')
    cur = conn.cursor()
    alist=", ".join(list(map(lambda item: str(item) ,  articleList)))
    cur.execute("UPDATE RawData SET country  ='United States' WHERE ID IN ("+alist+")  AND country ='USA'")
    conn.commit() 

    cur = conn.cursor()  
    cur.execute("UPDATE RawData SET country  ='United Kingdom' WHERE ID IN ("+alist+")  AND  country ='UK'")
    conn.commit()

    cur = conn.cursor()  
    cur.execute("UPDATE RawData SET ""Domain"" ='Geoeconomics' WHERE ID IN ("+alist+")  AND  ""Domain"" ='Geo-economics'")
    conn.commit()

    cur = conn.cursor()  
    cur.execute("UPDATE RawData SET ""Domain"" ='Others' WHERE ID IN ("+alist+")  AND  ""Domain"" IS NULL OR trim(""Domain"") =''") 
    conn.commit()

def process_content_to_get_issue(articleId):
    if IssueAlreadyExistsForArticle(articleId)==False:
        content=getContentFromRawData(articleId)
        outputcontent=llmAnalise(content)
        commitRawAnalysis(articleId,outputcontent)

def IssueAlreadyExistsForArticle(articleId):
    conn = sqlite3.connect('gidgirs_db')
    cur = conn.cursor()
    # SQL statement to select all data from the RawData table
    select_sql = 'SELECT 1 FROM RawData rd INNER JOIN RawAnalysis ra ON rd.ID =ra.ArticleID WHERE rd.ID='+str(articleId)  
     # Execute the SQL statement
    cur.execute(select_sql)

    # Fetch all rows from the query result
    rows = cur.fetchall()
    if len(rows) > 0:
          conn.close()
          return True

    return False

def SearchAlreadyExistsForArticle(searchString):
    conn = sqlite3.connect('gidgirs_db')
    cur = conn.cursor()
    # SQL statement to select all data from the RawData table
    select_sql = 'SELECT 1 FROM RawData rd WHERE rd.search_criteria ="'+str(searchString)+'"' 
    print(select_sql)
    # Execute the SQL statement
    cur.execute(select_sql)

    # Fetch all rows from the query result
    rows = cur.fetchall()
    if len(rows) > 0:
          conn.close()
          return True

    return False

#print(IssueAlreadyExistsForArticle(5038))

#process_content_to_get_issue(5038)

#tidy_up_countries([5014])
#clear_database_table("RawData")
#clear_database_table("RawAnalysis")