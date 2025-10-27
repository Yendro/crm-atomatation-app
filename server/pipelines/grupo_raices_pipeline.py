# cSpell:disable
"""
    Project: CRM Data Pipeline - Grupo Raices
    Author: Bussiness Intelligence Team
    Year: 2025
    Version: 2.0
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GrupoRaicesETL:
    """ETL processor for Grupo Raices sales data"""
    
    def __init__(self, input_path: str, output_path: str):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.dataframe: Optional[pd.DataFrame] = None
        
    def load_data(self) -> bool:
        """Carga el archivo Excel con manejo de errores"""
        try:
            logger.info(f"Cargando datos desde: {self.input_path}")
            
            if not self.input_path.exists():
                logger.error(f"Archivo no encontrado: {self.input_path}")
                return False
                
            self.dataframe = pd.read_excel(self.input_path)
            logger.info(f"Datos cargados exitosamente: {self.dataframe.shape[0]} filas, {self.dataframe.shape[1]} columnas")
            return True
            
        except Exception as e:
            logger.error(f"Error cargando archivo: {str(e)}")
            return False
    
    def validate_required_columns(self) -> bool:
        """Valida que las columnas requeridas estén presentes"""
        required_columns = ["Unidad", "M2", "Precio M2", "Asesor", "Cliente", 
                          "Precio Venta", "Fecha Carga contrato", "Status Venta", "Etapa"]
        
        missing_columns = [col for col in required_columns if col not in self.dataframe.columns]
        
        if missing_columns:
            logger.error(f"Columnas requeridas faltantes: {missing_columns}")
            return False
            
        logger.info("Todas las columnas requeridas están presentes")
        return True
    
    def filter_completed_sales(self) -> None:
        """Filtra solo las ventas con status 'Finalizado'"""
        initial_count = len(self.dataframe)
        self.dataframe = self.dataframe[self.dataframe["Status Venta"] == "Finalizado"]
        filtered_count = len(self.dataframe)
        
        logger.info(f"Ventas filtradas (Finalizadas): {filtered_count}/{initial_count} "
                   f"({(filtered_count/initial_count*100):.1f}%)")
    
    def standardize_names(self, column_name: str) -> None:
        """Estandariza nombres a formato título"""
        logger.info(f"Estandarizando nombres en columna: {column_name}")
        
        def format_name(name):
            if pd.isna(name):
                return name
            return str(name).title()
        
        self.dataframe[column_name] = self.dataframe[column_name].apply(format_name)
    
    def convert_to_first_day_month(self, date_column: str) -> None:
        """Convierte fechas al primer día del mes"""
        logger.info(f"Convertiendo fechas al primer día del mes: {date_column}")
        
        def first_day_of_month(date):
            if pd.isna(date):
                return date
            try:
                date_obj = pd.to_datetime(date)
                return date_obj.replace(day=1)
            except Exception as e:
                logger.warning(f"Error convirtiendo fecha {date}: {str(e)}")
                return date
        
        self.dataframe[date_column] = pd.to_datetime(self.dataframe[date_column], errors='coerce')
        self.dataframe[date_column] = self.dataframe[date_column].apply(first_day_of_month)
    
    def map_advisor_names(self) -> None:
        """Mapea nombres de asesores según diccionario definido"""
        logger.info("Mapeando nombres de asesores")
        
        advisor_mapping = {
            "Eq.": "Eq. Good Sales",
            "Alianza": "Alianza",
            "Grupo": "Grupo Jr",
            "Academia": "Academia",
            "Luis Alfonso": "Luis Alfonso",
            "Ivan Alberto": "Ivan Alberto",
            "Karla Soto": "Grupo Raices",
            "Carlos Humberto": "Carlos Humberto",
            "Carlos Daniel": "Carlos Daniel",
            "Margarita Eugenia": "Margarita Eugenia",
            "Jorge Alberto": "Jorge Alberto"
        }
        
        self.dataframe["Asesor"] = self.dataframe["Asesor"].replace(advisor_mapping)
    
    def assign_advisor_type(self) -> None:
        """Asigna tipo de asesor (Interno/Externo)"""
        logger.info("Asignando tipo de asesor")
        
        internal_advisors = [
            'Jorge Alberto', 'Margarita Eugenia', 'Carlos Daniel', 
            'Carlos Humberto', 'Grupo Raices', 'Ivan Alberto', 'Luis Alfonso'
        ]
        
        self.dataframe['Tipo'] = np.nan
        self.dataframe['Tipo'] = self.dataframe['Tipo'].astype('object')
        
        internal_mask = self.dataframe['Asesor'].isin(internal_advisors)
        self.dataframe.loc[internal_mask, 'Tipo'] = 'Interno'
        self.dataframe.loc[~internal_mask, 'Tipo'] = 'Externo'
        
        logger.info(f"Tipos asignados - Internos: {internal_mask.sum()}, "
                   f"Externos: {(~internal_mask).sum()}")
    
    def add_fixed_columns(self) -> None:
        """Añade columnas fijas requeridas por el modelo de datos"""
        logger.info("Añadiendo columnas fijas")
        
        self.dataframe['Marca'] = "Flamingo"
        self.dataframe['Desarrollo'] = "Flamingo"
        self.dataframe['Sucursal'] = 'Merida'
        self.dataframe['Modelo'] = 'No identificado'
        self.dataframe['Sub'] = np.nan
        self.dataframe['Hunter'] = np.nan
    
    def reorder_columns(self) -> None:
        """Reordena las columnas según el esquema final"""
        logger.info("Reordenando columnas")
        
        final_columns = [
            "Fecha", "Marca", "Desarrollo", "Sub", "Unidad", "Modelo", 
            "M2", "Precio M2", "Precio Venta", "Asesor", "Tipo", 
            "Hunter", "Cliente", "Sucursal"
        ]
        
        self.dataframe = self.dataframe.reindex(columns=final_columns)
    
    def rename_columns(self) -> None:
        """Renombra columnas según convención"""
        logger.info("Renombrando columnas")
        
        column_mapping = {
            'Fecha Carga contrato': 'Fecha'
        }
        
        self.dataframe.rename(columns=column_mapping, inplace=True)
    
    def save_results(self) -> bool:
        """Guarda el resultado en archivo Excel"""
        try:
            # Crear directorio si no existe
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            
            self.dataframe.to_excel(self.output_path, index=False)
            logger.info(f"Archivo guardado exitosamente: {self.output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error guardando archivo: {str(e)}")
            return False
    
    def get_summary(self) -> Dict:
        """Retorna un resumen del procesamiento"""
        if self.dataframe is None:
            return {}
            
        return {
            "total_records": len(self.dataframe),
            "columns": list(self.dataframe.columns),
            "date_range": {
                "min": self.dataframe["Fecha"].min(),
                "max": self.dataframe["Fecha"].max()
            },
            "advisors_count": self.dataframe["Asesor"].nunique(),
            "advisor_types": self.dataframe["Tipo"].value_counts().to_dict()
        }
    
    def run_pipeline(self) -> bool:
        """Ejecuta el pipeline completo de ETL"""
        logger.info("Iniciando pipeline de Grupo Raices")
        
        try:
            # Extract
            if not self.load_data():
                return False
                
            # Validate
            if not self.validate_required_columns():
                return False
            
            # Transform
            self.filter_completed_sales()
            self.standardize_names("Asesor")
            self.standardize_names("Cliente")
            self.add_fixed_columns()
            self.convert_to_first_day_month("Fecha Carga contrato")
            self.rename_columns()
            self.map_advisor_names()
            self.assign_advisor_type()
            self.reorder_columns()
            
            # Load
            if not self.save_results():
                return False
            
            # Summary
            summary = self.get_summary()
            logger.info(f"Pipeline completado - Resumen: {summary}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error en el pipeline: {str(e)}")
            return False


def main():
    """Función principal para ejecutar el pipeline"""
    input_file = "server/data/grupo-raices/Reporte_de_Flujo_Por_Desarrollo.xlsx"
    output_file = "server/data/grupo-raices/reporte_normalizado.xlsx"
    etl_processor = GrupoRaicesETL(input_file, output_file)
    
    success = etl_processor.run_pipeline()
    if success:
        print("✅ Pipeline ejecutado exitosamente")
        summary = etl_processor.get_summary()
        print(f"📊 Resumen: {summary['total_records']} registros procesados")
    else:
        print("❌ Error en el pipeline - Revisar logs")
        return 1
    return 0

if __name__ == "__main__":
    exit(main())