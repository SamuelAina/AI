import numpy as np
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
import keras

# TODO: fill out the function below that transforms the input series 
# and window-size into a set of input/output pairs for use with our RNN model
def window_transform_series(series, window_size):
    X = []
    y = []
    for i in range(0, len(series) - window_size):
        X.append(series[i:i+window_size])
        y.append(series[i+window_size])

    #convert X and y to numpy arrays 
    X = np.asarray(X)    
    y = np.asarray(y)
    
    #reshape y so that y=[?,?,?] becomes y=[[?],[?],[?]]
    y.shape = (len(y),1)
    return X,y

# TODO: build an RNN to perform regression on our time series input/output data
def build_part1_RNN(window_size):
    model = Sequential()
    model.add(LSTM(5, input_shape=(window_size, 1)))
    model.add(Dense(1))
    return model

### TODO: return the text input with only ascii lowercase and the punctuation given below included.
def cleaned_text(text): 
    #loop tru a whole bunch of funny characters to exclude them from the text
    funny_chars =['\n','\r','-','*','/','&','%','@','$','à','â','è','é','(',')','\xa0', '¢', '¨', '©', '»', '¿', 'ã', 'ï','#', '[', ']', '0', '1', '2','3', '4','5', '6', '7', '8', '9', '\\', '\'' ,'<' ,'>' ,'+', '^', '`', '~', '_','|' ,'\u000b', '\f', '\0x00', '\0x01', '\0x02', '\0x03', '\0x04', '\0x05', '\0x06', '\0x07', '\0x08', '\0x09', '\0x0A', '\0x0B', '\0x0C', '\0x0D', '\0x0E', '\0x0F', '\0x7F','\t','\a','\b','\0','=','"','{','}']
    for ch in funny_chars:
        text = text.replace(ch, ' ')
    return text

### TODO: fill out the function below that transforms the input text and window-size into a set of input/output pairs for use with our RNN model
def window_transform_text(text, window_size, step_size):
    # containers for input/output pairs
    inputs = []
    outputs = []
    for i in range(0, len(text) - window_size, step_size):
        inputs.append(text[i:i+window_size])
        outputs.append(text[i+window_size])        
    return inputs,outputs

# TODO build the required RNN model: 
# a single LSTM hidden layer with softmax activation, categorical_crossentropy loss 
def build_part2_RNN(window_size, num_chars):
    model = Sequential()
    model.add(LSTM(200, input_shape=(window_size, num_chars)))
    model.add(Dense(num_chars, activation='softmax'))
    optimizer = keras.optimizers.RMSprop(lr=0.001, rho=0.9, epsilon=1e-08, decay=0.0)
    model.compile(loss='categorical_crossentropy', optimizer=optimizer)
    return model