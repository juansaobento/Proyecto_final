import requests 
import json
import pandas as pd
import psycopg2 
import sqlalchemy as sa
import os
from dotenv import load_dotenv
from obtencion_datos import obtencion_datos

def creacion_dataframe():
    datalist= obtencion_datos()
    df= pd.DataFrame(datalist)[['idDrink','strDrink','strCategory','strAlcoholic','strGlass','strInstructions','strIngredient1', 'strIngredient2','strIngredient3', 'strIngredient4','strIngredient5','strIngredient6','strIngredient7','strIngredient8','strIngredient9','strIngredient10','strIngredient11','strIngredient12','strIngredient13','strIngredient14','strIngredient15','strMeasure1', 'strMeasure2', 'strMeasure3', 'strMeasure4', 'strMeasure5', 'strMeasure6', 'strMeasure7', 'strMeasure8', 'strMeasure9', 'strMeasure10', 'strMeasure11', 'strMeasure12', 'strMeasure13', 'strMeasure14', 'strMeasure15']]
    def create_ingredients_json(row):
        ingredients = {}
        for i in range(1, 16):  
            ingredient = row.get(f'strIngredient{i}')
            measure = row.get(f'strMeasure{i}')
            if ingredient and measure:
                ingredients[ingredient] = measure
        return json.dumps(ingredients)  
    df['Ingredientes'] = df.apply(create_ingredients_json, axis=1)
    df = df[['idDrink', 'strDrink', 'strCategory', 'strAlcoholic', 'strGlass', 'strInstructions', 'Ingredientes']]
    df= df.rename(columns= {'strDrink':'Cocktail', 'strCategory':'Categoria', 'strAlcoholic':'Alcoholico', 'strGlass':'Cristaleria', 'strInstructions':'Instrucciones'})
    return (df)
    