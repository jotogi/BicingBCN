# -*- coding: utf-8 -*-
"""Streamlit.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1Bg9oIOeL3FWXmbGRv5gRaOT19Sg1jeqi
"""

import streamlit as st
import pandas as pd
import shap
import matplotlib.pyplot as plt
from sklearn import datasets
#from sklearn.ensemble import TandomForestRegressor

st. write("""

# Bicing Prediction availability

This app predicts the **Bicing Availability**!
        
""")
st. write('---')

# Loads the Bicing Datasets
#bicing = datasets.load_bicing()
#X=pd.DataFrame(bicing.data, columns=bicing.feature_names)
#Y=pd.DataFrame(bicing.target, columns=['availability'])

#Sidebar
#Header of Specify Input Parameters
st.sidebar.header('Specify Input Parameters')

def user_input_features():
  ID_STATION = st.sidebar.slider('ID_STATION', X.ID_STATION.min(), X.ID_STATION.max(), X.ID_STATION.mean())




  data = {'ID_STATION': ID_STATION,
        
        }


  features = pd.DataFrame(data, index=[0])
  return features

# Main Panel

# Print specified input parameters
st.header('Specified Input parameters')
st.write(df)
st.write('---')

# Afegur features+columntransform
model = RandomForestRegressor()
model.fit(X, Y)

# Apply Model to Make Prediction
prediction = model.predict(df)

st.header('Prediction of availability')
st.write(prediction)
st.write('---')

# Explaining the model's predictions using SHAP values
# https://github.com/......

explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X)

st.header('Feature Importance')
st.title('Feature importance based on SHAP values')
shap.summary_plot(shap_values, X)
st.pyplot(bbox_inches='tight')
st.write('---')

plt.title('Feature importance based on SHAP values (Bar)')
shap.summary_plot(shap_values, X, plot_type="bar")
st.pyplot(bbox_inches='tight')
