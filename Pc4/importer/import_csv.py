import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys

class CSVToPostgres:
    def __init__(self):
        # Lee todas las configuraciones desde variables de entorno
        self.csv_path = os.getenv('CSV_PATH', '/app/data.csv')
        self.table_name = os.getenv('TABLE_NAME', 'bienes_servicios')
        self.chunk_size = int(os.getenv('CHUNK_SIZE', 10000))
        
        # Configuración de la base de datos
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'db_bienes_servicios'),
            'user': os.getenv('DB_USER', 'admin'),
            'password': os.getenv('DB_PASSWORD', 'admin')
        }
        
    def create_connection(self):
        """Crea la conexión con SQLAlchemy"""
        connection_string = (
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}"
            f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        )
        return create_engine(connection_string)
    
    def create_table(self, engine):
        """Crea la tabla con los tipos de datos correctos"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id SERIAL PRIMARY KEY,
            fecha_corte INTEGER NOT NULL,
            numero_ruc BIGINT NOT NULL,
            fecha_orden INTEGER NOT NULL,
            numero_orden BIGINT NOT NULL,
            codigo_generico VARCHAR(50),
            descripcion_generico TEXT,
            importe NUMERIC(15, 2),
            expediente_siaf INTEGER,
            fuente_financiamiento VARCHAR(100),
            especifico_gasto INTEGER,
            descripcion_especifico_gasto TEXT,
            cantidad NUMERIC(15, 2),
            tipo_bien VARCHAR(100),
            tipo_recurso VARCHAR(100),
            funcion_programa VARCHAR(100),
            meta_siaf INTEGER,
            programa INTEGER,
            subprograma INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_numero_ruc ON {self.table_name}(numero_ruc);
        CREATE INDEX IF NOT EXISTS idx_fecha_orden ON {self.table_name}(fecha_orden);
        CREATE INDEX IF NOT EXISTS idx_numero_orden ON {self.table_name}(numero_orden);
        """
        
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        print(f"Tabla '{self.table_name}' creada exitosamente")
    
    def clean_numeric_column(self, series):
        """Limpia y convierte columnas numéricas"""
        if series.dtype == 'object':
            # Elimina espacios, comas y convierte a float
            return pd.to_numeric(
                series.astype(str).str.replace(',', '').str.replace(' ', '').str.strip(),
                errors='coerce'
            )
        return series
    
    def prepare_dataframe(self, df):
        """Prepara el DataFrame con los tipos de datos correctos"""
        # Renombra columnas a snake_case
        df.columns = [
            'fecha_corte', 'numero_ruc', 'fecha_orden', 'numero_orden',
            'codigo_generico', 'descripcion_generico', 'importe',
            'expediente_siaf', 'fuente_financiamiento', 'especifico_gasto',
            'descripcion_especifico_gasto', 'cantidad', 'tipo_bien',
            'tipo_recurso', 'funcion_programa', 'meta_siaf', 'programa',
            'subprograma'
        ]
        
        # Limpia y convierte las columnas numéricas que están como object
        df['importe'] = self.clean_numeric_column(df['importe'])
        df['cantidad'] = self.clean_numeric_column(df['cantidad'])
        
        # Asegura que las columnas de texto no tengan valores nulos
        text_columns = ['codigo_generico', 'descripcion_generico', 
                       'fuente_financiamiento', 'descripcion_especifico_gasto',
                       'tipo_bien', 'tipo_recurso', 'funcion_programa']
        
        for col in text_columns:
            df[col] = df[col].fillna('').astype(str)
        
        return df
    
    def load_data(self):
        """Carga los datos del CSV a PostgreSQL"""
        try:
            print(f"Leyendo CSV desde: {self.csv_path}")
            df = pd.read_csv(self.csv_path)
            print(f"Total de registros leídos: {len(df)}")
            
            # Prepara el DataFrame
            df = self.prepare_dataframe(df)
            
            # Crea la conexión
            engine = self.create_connection()
            print("Conexión a PostgreSQL establecida")
            
            # Crea la tabla
            self.create_table(engine)
            
            # Carga los datos en chunks
            print(f"Cargando datos en chunks de {self.chunk_size} registros...")
            df.to_sql(
                self.table_name,
                engine,
                if_exists='append',
                index=False,
                chunksize=self.chunk_size,
                method='multi'
            )
            
            print(f"✓ Datos cargados exitosamente: {len(df)} registros")
            
            # Verifica la carga
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.table_name}"))
                count = result.scalar()
                print(f"Total de registros en la tabla: {count}")
            
            return True
            
        except Exception as e:
            print(f"✗ Error durante la carga: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

def main():
    loader = CSVToPostgres()
    success = loader.load_data()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()