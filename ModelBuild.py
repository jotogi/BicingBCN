# Desem els parametres 

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense


data = average_df
data_predict = metadata_sample_submission
test_size = 0.2
max_iter = 700
features = ['station_id', 'month', 'day', 'hour', 'ctx-4', 'ctx-3', 'ctx-2','ctx-1'] 
to_predict = ['relative occupancy']
number_of_dense = 4
number_of_neurons = [25, 10, 15, 26]
activation = 'relu'
loss = 'mean_square_error'
optimizer = 'adam'
epochs = 20
batch_size = 500

# Creem els conjunts de dades, les separem amb el train_test_split, i el parametre de test size per a 
def build_train_test (data = data, features = features, to_predict = to_predict, test_size = test_size)
    X = data[features]
    y = data[to_predict]

    X_train, X_test, y_train, y_test = train_test_split(X,y, test_size=test_size)

    scaler = StandardScaler()

    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.fit_transform(X_test)
    logger.debug('Test and train sets -> Created!')  
    return X_train_scaled, X_test_scaled, y_train, y_test


def build_model (X_train_scaled = X_train_scaled, X_test_scaled = X_test_scaled, y_train = y_train, y_test = y_test, number_of_dense = number_of_dense, activation = activation, loss = loss, optimizer = optimizer, epochs = epochs, batch_size = batch_size)
    model = Sequential
    model.add(Dense(number_of_neurons[0], activation = activation, input_shape = (len(features),)))
    for i in range(1, number_of_dense - 1):
        model.add(Dense(number_of_dense[i], activation = activation, input_shape = (len(features))))
    model.compile(loss = loss, optimizer = optimizer, metrics = loss)
    model.fit(X_train_scaled, y_train, epochs = epochs, batch_size = batch_size)
    loss1, mse = model.evaluate(X_test_scaled, y_test)
    logger.debug('Model -> fitted!')
    logger.debug(f'the loss of the model is' loss1)

    return model


def predict_model (model = model,  data_predict = data_predict, features = features):
    predict_features = data_predict[features]
    submission_features_scaled = scaler.transform(predict_features)
    predictions = model.predict(submission_features_scaled)
    data_submit = data_predict
    data_submit['percentage_docks_available'] = predictions
    data_submit = data_submit[['Index', 'percentage docks available']]
    logger.debug('Data to submit -> Created!')  
    return data_submit

if __name__ == '__main__':
    main()