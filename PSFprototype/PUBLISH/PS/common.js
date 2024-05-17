var app_session={
    LOGED_IN:false
};

function extractFormDataToLoc(formId) {
    var form = document.getElementById(formId);
    var elements = form.elements;
    var formData = {};
    for (var i = 0; i < elements.length; i++) {
        var element = elements[i];
        if (element.id && element.type !== "submit") {
			if(element.type=="checkbox"){
			   formData[element.id] = element.checked;
			}else{
			   formData[element.id] = element.value;
			}			
        }
    }
	saveToLocalStorage(formId, formData);
	console.log("saveToLocalStorage",formId,formData);
    return JSON.stringify(formData);
}

function repopulateFormDataFromLoc(key){
  formData = loadFromLocalStorage(key);
  if(! formData){
    return
  }
  
  var form = document.getElementById(key);
  var elements = form.elements;
     for (var i = 0; i < elements.length; i++) {
        var element = elements[i];
        if (formData[element.id]) {
            element.value = formData[element.id];
			if(element.type=="checkbox"){
			   element.checked = formData[element.id];
			}else{
			   element.value = formData[element.id];
			}			
        }
    } 
}


function loadFromLocalStorage(key) {
	const jsonString = localStorage.getItem(key.toUpperCase());
	return JSON.parse(jsonString);
}

function saveToLocalStorage(key, jsonData) {
	const jsonString = JSON.stringify(jsonData);
	localStorage.setItem(key.toUpperCase(), jsonString);
}

function clearLocalStorage(key) {
	localStorage.removeItem(key);
}

function mainPage(){
    window.location.href = "Features.html"; 
}

function checkLoggedIn(){
	app_session=loadFromLocalStorage("app_session");
	if (!app_session){
		app_session={LOGED_IN:false};
		window.location.href = "User login.html";
	}
	if(!app_session.LOGED_IN){
		window.location.href = "User login.html";
	}        
}