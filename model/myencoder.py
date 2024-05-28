import tensorflow as tf
from tensorflow import keras
from keras import Model, layers
import numpy as np
import warnings
warnings.filterwarnings("ignore")

def positional_encoding(length, depth):
  depth = depth/2
  positions = np.arange(length)[:, np.newaxis]     # (seq, 1)
  depths = np.arange(depth)[np.newaxis, :]/depth   # (1, depth)
  angle_rates = 1 / (10000**depths)         # (1, depth)
  angle_rads = positions * angle_rates      # (pos, depth)
  pos_encoding = np.concatenate(
      [np.sin(angle_rads), np.cos(angle_rads)],
      axis=-1) 
  return tf.cast(pos_encoding, dtype=tf.float32)


def transformer_encoder(inputs, head_size, num_heads, dropout=0):
    dim = inputs.shape[-1]
    # Normalization and Attention
    x = layers.LayerNormalization(epsilon=1e-6)(inputs)
    x = layers.MultiHeadAttention(
        key_dim=head_size, num_heads=num_heads, dropout=dropout
    )(x, x)
    x = layers.Dropout(dropout)(x)
    res = x + inputs

    # Feed Forward Part
    x = layers.LayerNormalization(epsilon=1e-6)(res)
    x = layers.Dense(dim, activation="relu")(x)
    x = layers.Dropout(dropout)(x)
    x = layers.Dense(dim, activation="relu")(x)
    return x + res


# prepare model
def make_model(
    input_shape, head_size, num_heads, num_transformer_blocks, dropout=0) -> Model:
    sequence_length = input_shape[-2]
    embed_dim = input_shape[-1]
    model_input = keras.Input(shape= input_shape)
    encoder_input = tf.keras.layers.LSTM(16, return_sequences=True)(model_input)
    #x = encoder_input
   
    # add positional encoding
    x = encoder_input + positional_encoding(sequence_length, 16)  #embed_dim
    
    for _ in range(num_transformer_blocks):
        x = transformer_encoder(x, head_size, num_heads, dropout)

    x = layers.Flatten()(x)
    #x = layers.GlobalAveragePooling1D(data_format='channels_first')(x)

    dense = layers.Dense(8, activation='relu')(x)
    # for dim in mlp_units:
    #     x = layers.Dense(dim, activation="relu")(x)
    #     x = layers.Dropout(mlp_dropout)(x)
    outputs = layers.Dense(1)(dense)
    model = keras.Model(model_input, outputs)
    return model
  
   
    
class encoder_regression:
    def __init__(self, shape, head_size=16, num_heads=4, num_transformer_blocks=2):
        self.shape = shape
        self.head_size = head_size
        self.num_heads = num_heads
        self.num_transformer_blocks = num_transformer_blocks

        self.model = make_model(self.shape, self.head_size, self.num_heads, self.num_transformer_blocks)


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


    def predict(self, x):
        return self.model.predict(x)
    



