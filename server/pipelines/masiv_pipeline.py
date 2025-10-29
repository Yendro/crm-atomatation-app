import pandas as pd
import re

df1= pd.read_excel('server/data/masiv/Reporte_de_Flujo_Por_Desarrollo.xlsx')
df2= pd.read_excel('server/data/masiv/Reporte_de_Flujo_Por_Desarrollo2.xlsx')

df3 = pd.concat([df1, df2]).drop_duplicates().reset_index(drop=True)

columnas_finales = [
        "Desarrollo","Unidad", "M2", "Precio M2", "Asesor", "Cliente", "Precio Venta", "Fecha Carga contrato", "Status Venta", "Etapa"]
df3 = df3.reindex(columns=columnas_finales)#Reordenar columnas
df3 = df3[df3["Status Venta"] == "Finalizado"]


        
        
        
def limpiar_desarrollo(nombre):    
    if pd.isna(nombre):
        return nombre
    
    # Caso especial: conservar "Claro de Mar" completo
    if "Claro de Mar" in nombre:
        return "Claro de Mar"
    
    # Eliminar patrones como "FRACCION 2", "FRACCION 3", etc.
    patron = r'FRACCION\s*\d+\s*'
    nombre_limpio = re.sub(patron, '', nombre, flags=re.IGNORECASE)
    
    # Eliminar espacios extra al inicio y final
    return nombre_limpio.strip()

def convertir_a_primer_dia_mes(fecha):
    if pd.isna(fecha):
        return fecha
    # Convertir al primer día del mes
    return fecha.replace(day=1)


    #Estandarizar nombres con minusculas 
def estandarizar_nombre(nombre):
    if pd.isna(nombre):
        return nombre
    nombre = nombre.title()  # Convertir a título (primera letra en mayúscula)
    return nombre


def quitar_acentos(texto):
    acentos = {'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
            'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U'}
    for acento, sin_acento in acentos.items():
        texto = texto.replace(acento, sin_acento)
    return texto if texto not in ('nan', 'NaT') else ''
    

df3["Desarrollo"] = df3["Desarrollo"].apply(str).apply(quitar_acentos)



df3['Asesor'] = df3['Asesor'].apply(estandarizar_nombre)
df3['Cliente'] = df3['Cliente'].apply(estandarizar_nombre)
df3['Desarrollo'] = df3['Desarrollo'].apply(limpiar_desarrollo)


df3['Fecha Carga contrato'] = pd.to_datetime(df3['Fecha Carga contrato'], errors='coerce')
df3['Fecha Carga contrato'] = df3['Fecha Carga contrato'].apply(convertir_a_primer_dia_mes)
df3['Desarrollo'] = df3['Desarrollo'].apply(estandarizar_nombre)


cond = df3["Desarrollo"] == "Claro De Mar"
etapa_temp = df3.loc[cond, "Etapa"]

# # Intercambiamos
df3.loc[cond, "Desarrollo"] = etapa_temp
df3.loc[cond, "Etapa"] = "Claro De Mar"


columnas_renombrar = {'Fecha Carga contrato':'Fecha',  'Desarrollo':'Sub',}
df3.rename(columns= columnas_renombrar, inplace=True)

mapa_nombres = {
    "Monserrat": "Monserrat Malja",
    "Gerardo": "Gerardo de la Peña",
    "Said": "Said Ortiz",
    "Yoskua": "Yoskua Amaro",
    "Armando": "Armando Gonzalez",
    "Cynthia": "Cynthia Aguilar",
    "Evelyn": "Evelyn",
    "Miguel": "Miguel Benavente",
    "Luis": "Luis Ruiz",
    "Iliana": "Iliana Gómez"
}

# Reemplazar en la columna
df3["Asesor"] = df3["Asesor"].replace(mapa_nombres)

df3['Sucursal'] = 'Merida'
df3['Tipo'] = 'Interno'
df3['Equipo'] = 'N/A'
df3['Modelo'] = 'No identificado'
df3['Marca'] = 'Navire'



df3['Desarrollo'] = df3.apply(lambda row: row['Etapa'] if row['Etapa'] == 'Claro De Mar' else 'Navire', axis=1)

columnas_finales = [
        "Fecha","Marca", "Desarrollo", "Sub", "Unidad", "Modelo", "M2", "Precio M2", "Precio Venta", "Asesor", "Sucursal", "Tipo", "Equipo", "Cliente"]

df3 = df3.reindex(columns=columnas_finales)#Reordenar columnas


df3.to_excel('server/data/masiv/reporte-normalizado-masiv.xlsx', index=False)