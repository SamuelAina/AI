# main.py
from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
import gidgirs
def create_app():
    app = Flask(__name__)
    CORS(app)  # Enables CORS for all routes
    return app

app = create_app()

@app.route('/getIssuesInfo', methods=['POST'])
def getIssuesInfo():
    return jsonify({"result": "OK"})

@app.route('/getCountryList', methods=['POST'])
def getCountryList():
    conn = sqlite3.connect('gidgirs_db')
    cur = conn.cursor()
    # SQL statement to select all data from the RawData table
    select_sql = """
    SELECT '(none)' AS country 
    UNION 
    SELECT DISTINCT countryName 
    FROM Countries c  
    ORDER BY country
    """

    # Execute the SQL statement
    cur.execute(select_sql)

    # Fetch all rows from the query result
    rows = cur.fetchall()

    # Close the database connection
    conn.close()
    return rows    

@app.route('/getArticles', methods=['POST'])    
def getArticles():
    mydata = request.get_json()
    #print(mydata, mydata["DateRange"])


    if mydata["DateRange"] == 'In the past day':
        startdate, enddate = gidgirs.resolve_daterange("past day")
    elif mydata["DateRange"] == 'In the past week':
        startdate, enddate = gidgirs.resolve_daterange("past week")
    elif mydata["DateRange"] == 'In the past month':
        startdate, enddate = gidgirs.resolve_daterange("past month")
    elif mydata["DateRange"] == 'In the past year':
        startdate, enddate = gidgirs.resolve_daterange("past year")
    elif mydata["DateRange"] == 'Specific date range':
        try:
            #print(mydata["SpecificStartDate"],mydata["SpecificEndDate"])
            #startdate, enddate=gidgirs.reformat_date(mydata["SpecificStartDate"]),gidgirs.reformat_date(mydata["SpecificEndDate"])
            startdate, enddate=mydata["SpecificStartDate"],mydata["SpecificEndDate"]
            #print(startdate, enddate)
        except:
            startdate, enddate="4040-01-01","4040-01-01"
    else:
        daterang='This is the default case'   
    
    country= mydata["Country"]
    domainlist=", ".join(list(map(lambda item: "'"+item + "'",  mydata["Domain"])))
    


    conn = sqlite3.connect('gidgirs_db')
    cur = conn.cursor()

    if mydata["CurrentView"]=="articles":
        select_sql = """
        SELECT DISTINCT
        rd.title,
        strftime('%d', rd.publishedAt) || ' ' ||
        CASE strftime('%m', rd.publishedAt)
            WHEN '01' THEN 'Jan'
            WHEN '02' THEN 'Feb'
            WHEN '03' THEN 'Mar'
            WHEN '04' THEN 'Apr'
            WHEN '05' THEN 'May'
            WHEN '06' THEN 'Jun'
            WHEN '07' THEN 'Jul'
            WHEN '08' THEN 'Aug'
            WHEN '09' THEN 'Sep'
            WHEN '10' THEN 'Oct'
            WHEN '11' THEN 'Nov'
            WHEN '12' THEN 'Dec'
        END || ' ' ||
        strftime('%Y', rd.publishedAt) AS formatted_date,  
        rd.Summary,  
        rd.url,     
        CASE WHEN rd."Domain" NOT IN ('Environmental' ,'Social' ,'Political' ,'Geoeconomics' ,'Technology' ,'Others') THEN 'Others' ELSE rd."Domain" END AS "Domain",
        rd.ID AS ArticleID,
        rd.Country
        FROM RawData rd
        LEFT JOIN RawAnalysis ra 
        ON rd.ID =ra.ArticleID 
        WHERE CASE WHEN rd."Domain" NOT IN ('Environmental' ,'Social' ,'Political' ,'Geoeconomics' ,'Technology' ,'Others') THEN 'Others' ELSE rd."Domain" END IN ("""+domainlist+""")
        AND date(rd.publishedAt)  BETWEEN '"""+startdate+"""' AND '"""+enddate+"""'  
        AND rd.Country IN ('"""+country+"""')
        ORDER BY rd.publishedAt DESC
        """

        print(select_sql)
    else:
        if mydata["CurrentView"]=="issues":
            select_sql = """
                SELECT 
                ra.JSON_Data,
                CASE WHEN rd."Domain" NOT IN ('Environmental' ,'Social' ,'Political' ,'Geoeconomics' ,'Technology' ,'Others') THEN 'Others' ELSE rd."Domain" END AS "Domain",
                rd.ID AS ArticleID
                FROM RawData rd
                INNER JOIN RawAnalysis ra 
                ON rd.ID =ra.ArticleID 
                WHERE CASE WHEN rd."Domain" NOT IN ('Environmental' ,'Social' ,'Political' ,'Geoeconomics' ,'Technology' ,'Others') THEN 'Others' ELSE rd."Domain" END IN ("""+domainlist+""")
                AND date(rd.publishedAt)  BETWEEN '"""+startdate+"""' AND '"""+enddate+"""'   
                AND rd.Country IN ('"""+country+"""')                 
            """


    # Execute the SQL statement
    cur.execute(select_sql)

    # Fetch all rows from the query result
    rows = cur.fetchall()

    # Close the database connection
    conn.close()
    return rows  


@app.route('/getArticlesForSearchByCountryAndDate', methods=['POST']) 
def getArticlesForSearchByCountryAndDate():
    mydata = request.get_json()
    #print(mydata)

    daterange={"start_date":gidgirs.reformat_date(mydata["SpecificStartDate"]),"end_date":gidgirs.reformat_date(mydata["SpecificEndDate"])}
    country=mydata["Country"]

    gidgirs.getArticlesForSearchByCountryAndDate(daterange,country)
    return {"result":"OK"}

@app.route('/process_content_to_get_issue', methods=['POST']) 
def process_content_to_get_issue():
    mydata = request.get_json()
    #print(mydata)
    articleId=mydata["ArticleID"]

    gidgirs.process_content_to_get_issue(articleId)
    return {"result":"OK"}


@app.route('/fetchRelevantURLsFromWeb', methods=['POST']) 
def fetchRelevantURLsFromWeb():
    mydata = request.get_json()
    #print(mydata)

    daterange={"start_date":gidgirs.reformat_date(mydata["SpecificStartDate"]),"end_date":gidgirs.reformat_date(mydata["SpecificEndDate"])}
    country=mydata["Country"]
    search_text="(("+country+") AND (Geo economic OR political OR environmental OR social OR technology))"
    
    savedarticles=gidgirs.gatherRawContentForCountries(search_text, daterange)
    return savedarticles
    

@app.route('/fetchArticleContentsFromWeb', methods=['POST']) 
def fetchArticleContentsFromWeb():
    mydata = request.get_json()
    savedarticles=mydata["savedarticles"]
    gidgirs.update_missing_contents(savedarticles) 
    return savedarticles

@app.route('/determineCountryRelevanceForArticles', methods=['POST']) 
def determineCountryRelevanceForArticles():
    mydata = request.get_json()
    savedarticles=mydata["savedarticles"]
    gidgirs.update_missing_countries_and_domains(savedarticles)
    gidgirs.tidy_up_countries(savedarticles)        
    return savedarticles

@app.route('/generateSummaryForArticles', methods=['POST']) 
def generateSummaryForArticles():
    mydata = request.get_json()
    savedarticles=mydata["savedarticles"]
    gidgirs.update_missing_summaries(savedarticles)
    return {"result":"OK"}


# Main entry point for running the app
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)

  