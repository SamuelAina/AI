from flask import Flask, request, render_template
from keras.models import model_from_json
import numpy as np
from keras import backend
app = Flask(__name__)

_surname = "" 
_firstname=""

char_dict={ " ":0 ,"a":1 ,"b":2 ,"c":3 ,"d":4 ,"e":5,"f":6
		   ,"g":7 ,"h":8 ,"i":9 ,"j":10
		   ,"k":11,"l":12,"m":13,"n":14,"o":15,"p":16
		   ,"q":17,"r":18,"s":19,"t":20,"u":21
		   ,"v":22,"w":23,"x":24,"y":25,"z":26,"-":27}
	
@app.route("/",  methods=['GET','POST'])
def index():
	return render_template("index.html",  title="Country from Fierstname and Surname");
	
@app.route('/main',  methods=['GET','POST'])
def main():
	global _surname
	global _firstname

	if request.form.get('surname') != None:
		_surname=request.form.get('surname')
		
	if request.form.get('firstname') != None:
		_firstname=request.form.get('firstname')
	
	return  render_template(
	"main.html",  
	title="Country from Firstname and Surname",
	_firstname=_firstname,
	_surname=_surname,
	predicted_country= predict(_firstname, _surname)	
	);


def firstname_surname_encoded(firstname = 'babatunde',surname='----', fixed_width1=30, fixed_width2=30):
	global char_dict	
	string_list = list(firstname) + ['-']*(fixed_width1-len(firstname))\
			  + list(surname) + ['-']*(fixed_width2-len(surname))

	encoded_all_chars  = [[1 if char_dict.get(ch)==i else 0 for i in range(28)] for ch in string_list]  
 
	return encoded_all_chars


def predict(firstname, surname):
	global char_dict
	backend.clear_session()
	json_file = open('.\Model\madeup_data_model.json', 'r')
	loaded_model_json = json_file.read()
	json_file.close()
	loaded_model = model_from_json(loaded_model_json)
	
	# load weights into new model
	loaded_model.load_weights(".\Model\madeup_data_model.h5")	
	
	#our model only works with lower case letter
	firstname =firstname.lower()
	surname = surname.lower()
	encoded_input=firstname_surname_encoded(firstname =firstname,surname=surname, fixed_width1=30, fixed_width2=30)
	
	np_encoded_input=np.array(encoded_input)
	
	np_encoded_input=np.reshape(np_encoded_input, (1,60, 28))
	
	prediction = loaded_model.predict(np_encoded_input)

	idx_of_best_match=prediction.argsort()[0][-1:][0]    
	bestmatch=list(char_dict.keys())[list(char_dict.values()).index(idx_of_best_match)]," - ", prediction[0][idx_of_best_match]        
	return bestmatch

if __name__ == "__main__":	
	app.run(host='127.0.0.1', port=5001, debug=True)