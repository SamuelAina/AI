from flask import Flask

app = Flask(__name__)

@app.route("/", methods=['GET', 'POST'])
def hello():
    return  '''
    First Name:<input type='textbox'></input><br/>
       Surname:<input type='textbox'></input><br/>
               <input type='submit' value='OK'/>
'''


if __name__ == "__main__":
    app.run()