# -*- coding: utf-8 -*-
"""Bicing_Streamlit.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1HlHZNw_l4sbW2nZphYhGWeZAaOGIXriu

# APP STREAMLIT
"""

# ANAR A CONSOLA.

# !pip install colab_xterm
# %load_ext colabxterm
# %xterm

from google.colab import drive
drive.mount('/content/drive')

!pip install streamlit
# !pip install streamlit -q!

pip install shap

pip install ipykernel

pip install pyngrok

# Commented out IPython magic to ensure Python compatibility.
# %%writefile bicing_streamlit.py
# import streamlit as st
# import xgboost as xgb
# from joblib import load
# 
# # Load the trained XGBoost model
# model = xgb.XGBRegressor(n_estimators=400)
# # model = load('/content/bicing_xgb_model.joblib')
# model = load('/content/drive/MyDrive/Bicing/bicing_xgb_model.joblib')
# 
# # Create the Streamlit app
# def main():
# 
#     # Provide the URL of the image
#     image_url = "https://www.viaempresa.cat/uploads/s1/26/08/67/67/una-estacio-de-servei-del-bicing-de-barcelona-acn_11_640x380.jpeg"
# 
#     # Display the image
#     st.image(image_url, caption='Remote Image', use_column_width=True)
# 
#     st.title("Bicing Prediction")
# 
#     # Input features
#     id_station = st.slider("ID_STATION", 1, 519, 1)
#     hour=st.slider("HOUR", 1, 24, 1)
#     day = st.slider("DAY", 1, 31, 1)
#     month = st.slider("MONTH", 1, 12, 1)
# 
#     # Create a feature vector from the inputs
#     features = [[id_station, hour, day, month]]
# 
#     # Make predictions using the loaded XGBoost model
#     # dmatrix = xgb.DMatrix(features)
#     predictions = model.predict(features)
# 
#     # Display the predicted class
#     predicted = float(predictions[0])
#     st.write("THE AVAILABILITY PREDICTION IS:", predicted)
# 
# if __name__ == '__main__':
#     main()

!streamlit run bicing_streamlit.py &>/dev/null&

!ngrok authtoken 2Lk1zND98AOSuJZvbSHwzLvM86b_5ELs8CJH7zEYFDq5LwVMr

!wget https://bin.equinox.io/c/4VmDzA7iaHb/ngrok-stable-linux-amd64.zip

!unzip ngrok-stable-linux-amd64.zip

from IPython import get_ipython

get_ipython().system_raw('./ngrok http 8501 &')

! curl -s http://localhost:4040/api/tunnels

!streamlit run /content/bicing_streamlit.py