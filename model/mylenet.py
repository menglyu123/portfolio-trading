import tensorflow as tf
from tensorflow import keras
from keras import Model, layers
from sklearn.utils import class_weight
import numpy as np
import warnings
warnings.filterwarnings("ignore")

   
    
class lenet_regression:
    def __init__(self, shape):
        self.shape = shape
        input = keras.Input(shape=shape)
        conv1 = tf.keras.layers.Conv1D(filters=32, kernel_size=7, activation='relu')(input)
        pool1 = tf.keras.layers.AveragePooling1D(pool_size=3, strides=1, padding='same')(conv1)

        conv2 = tf.keras.layers.Conv1D(filters=16, kernel_size=5, activation='relu')(pool1)
        pool2 = tf.keras.layers.AveragePooling1D(pool_size=3, strides=1, padding='same')(conv2)

        conv3 = tf.keras.layers.Conv1D(filters=16, kernel_size=3, activation='relu')(pool2)
        pool3 = tf.keras.layers.AveragePooling1D(pool_size=3, strides=1, padding='same')(conv3)
        
        # lstm = tf.keras.layers.LSTM(16, return_sequences=True)(pool3)
        # lstm = tf.keras.layers.Flatten()(lstm)
        # lstm = tf.keras.layers.Dropout(0.4)(lstm)

        lstm = tf.keras.layers.LSTM(16)(pool3)
        dense2 = tf.keras.layers.Dense(8, activation='relu')(lstm)
        output = tf.keras.layers.Dense(1)(dense2)
        self.model = tf.keras.Model(input, output)


    #------ Function for training model ------
    def train(self, x, y, val_x, val_y, bs, lr, epochs, path):
        optimizer = tf.keras.optimizers.Adam(learning_rate= lr)
        self.model.compile(
            optimizer = optimizer, 
            loss= tf.keras.losses.MeanSquaredError(), 
            metrics=[
                tf.keras.metrics.MeanSquaredError(name='mse')]
            )
        checkpoint_filepath = f'{path}/epoch_'+'{epoch:02d}'+'.h5'
        callback = tf.keras.callbacks.ModelCheckpoint(
            filepath = checkpoint_filepath,
            monitor = 'val_loss',
            mode = 'min',
            save_best_only= False,  
            save_weights_only=True,
            verbose = 1
        )
    
        # fit model
        history = self.model.fit(x, y, validation_data =(val_x, val_y), batch_size = bs, epochs= epochs, callbacks=[callback])
        
        # plot training performance and validation performance
        print(history.history['loss'],history.history['val_loss'] )
        import matplotlib.pyplot as plt
        plt.figure(figsize=(10,7))
        plt.plot(history.history['loss'], c='blue', label='training')
        plt.plot(history.history['val_loss'],c='red', label='testing')
        plt.legend()
        plt.title("loss function value")
        plt.savefig('./result/training&val_performance_transformer_regression.jpg')
    #------------------------------------------


    def evaluate(self, x, y):
        self.model.evaluate(x, y, verbose=2)


    def load(self, path):
        self.model.load_weights(path)    
    
    def save(self, path):
        self.model.save(path)


    def predict(self, x):
        return self.model.predict(x)