# Databricks notebook source
dbutils.widgets.text("id_download_batch", "")

# COMMAND ----------

# MAGIC %pip install pandas shapely geoalchemy2 psycopg2 folium calplot

# COMMAND ----------

import io
import urllib
import folium
import calplot
import shapely
import psycopg2
import itertools
import numpy as np
import pandas as pd
import geopandas as gpd
from dataclasses import dataclass
from collections import OrderedDict
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from pandas.core.indexes.numeric import Int64Index
from sqlalchemy.engine.cursor import LegacyCursorResult
from typing import Tuple, Union, Iterable, NoReturn, Sequence, Any, Mapping, List, Union, Callable, TextIO

# COMMAND ----------

jdbcHostname = dbutils.secrets.get(scope='secretgdm-scope', key='SQL-PBS-Host')
jdbcPort = dbutils.secrets.get(scope='secretgdm-scope', key='SQL-PBS-Port')
jdbcDatabase = dbutils.secrets.get(scope='secretgdm-scope', key='SQL-PBS-Database-Inv')
user_db = dbutils.secrets.get(scope='secretgdm-scope', key='SQL-PBS-User')
pass_db = dbutils.secrets.get(scope='secretgdm-scope', key='SQL-PBS-Pass')

CURRENT_ID_DOWNLOAD_BATCH: int = int(dbutils.widgets.get('id_download_batch').strip())
# PBS_DB_CONNECTION_STRING: str = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};user={user_db};password={pass_db};encrypt=false;driver=com.microsoft.sqlserver.jdbc.SQLServerDriver;loginTimeout=30;"

DB_QUERY_SCHEMAS: Mapping[str, str] = {
    'incremental': 'config',
    'historical': 'int' 
}

REQUIRED_SCHEMA_FOR_QUERY: Sequence[str] = ['IdEnsayo', 'NombreEnsayo', 'IdAnio', 'pais', 'micro', 'macro', 'TipoSemilla', 'localidad', 'LocalidadLatitud', 'LocalidadLongitud', 'establecimiento', 'EstablecimientoLatitud', 'EstablecimientoLongitud', 'lote', 'LoteLatitud', 'LoteLongitud', 'EnsayoLatitud', 'EnsayoLongitud', 'FechaDeSiembra', 'CreatedAt', 'UpdatedAt']
REQUIRED_SCHEMA_FOR_TABLE: Sequence[str] = ['IdEnsayo', 'NombreEnsayo', 'IdAnio', 'pais', 'micro', 'macro', 'TipoSemilla', 'localidad', 'LocalidadLatitud', 'LocalidadLongitud', 'establecimiento', 'EstablecimientoLatitud', 'EstablecimientoLongitud', 'lote', 'LoteLatitud', 'LoteLongitud', 'EnsayoLatitud', 'EnsayoLongitud', 'FechaDeSiembra', 'CreatedAt', 'UpdatedAt', 'latitud', 'longitud', 'fuente', 'fuente_de_coordenadas', 'id_download_batch']

DEFAULT_FECHA_DE_SIEMBRA: datetime = datetime(1980, 1, 1)

COORDINATES_FIELDS: Mapping[str, Sequence[str]] = OrderedDict()
COORDINATES_FIELDS['ensayo'] = ['EnsayoLongitud', 'EnsayoLatitud']
COORDINATES_FIELDS['lote'] = ['LoteLongitud', 'LoteLatitud']
COORDINATES_FIELDS['establecimiento'] = ['EstablecimientoLongitud', 'EstablecimientoLatitud']
COORDINATES_FIELDS['localidad'] = ['LocalidadLongitud', 'LocalidadLatitud']

COORDINATES_FIELDS_FLAT: List[str] = list(itertools.chain.from_iterable(COORDINATES_FIELDS.values()))
NON_NULLABLE_FIELDS: List[str] = ['IdEnsayo', 'CreatedAt', 'pais', 'macro', 'micro', 'TipoSemilla']
FINAL_LONGITUDE_COLUMN = 'longitud'
FINAL_LATITUDE_COLUMN = 'latitud'
NEGATIVE_COORDINATE_FIXING = lambda value: -abs(value)
POSITIVE_COORDINATE_FIXING = lambda value: abs(value)
COORDINATES_SIGNS_BY_COUNTRY: Mapping[str, Mapping[str, Callable[[Union[int, float]], Union[int, float]]]] = {
    'ARGENTINA': {'longitud': NEGATIVE_COORDINATE_FIXING, 'latitud': NEGATIVE_COORDINATE_FIXING},
    'BRASIL':    {'longitud': NEGATIVE_COORDINATE_FIXING, 'latitud': NEGATIVE_COORDINATE_FIXING},
    'URUGUAY':   {'longitud': NEGATIVE_COORDINATE_FIXING, 'latitud': NEGATIVE_COORDINATE_FIXING},
    'PARAGUAY':  {'longitud': NEGATIVE_COORDINATE_FIXING, 'latitud': NEGATIVE_COORDINATE_FIXING},
    'USA':       {'longitud': NEGATIVE_COORDINATE_FIXING, 'latitud': POSITIVE_COORDINATE_FIXING},
}

# COMMAND ----------

class NoValuesError(Exception):
    pass


class CoordinatesError(Exception):
    pass

# COMMAND ----------

@dataclass
class DFPipeline:
    max_update_at_sink: Union[None, str] = None
    
    def __post_init__(self):
        self.df = None
        
        # Request data from PBS db
        self.get_essays_from_pbs()
        
        # Pre-process & clean up
        self.check_if_dataframe_has_required_schema()
        self.pre_process_essays_dataframe()
        self.drop_rows_without_any_coordinate()
        self.drop_rows_with_empty_mandatory_values()
        
        # Coordinates fixing
        self.choose_final_pair_of_coordinates()
        self.fix_exchanged_coordinates()
        self.fix_bad_signed_coordinates()
        self.discard_out_of_range_coordinates()
        
        # Post-process
        self.post_process_essays_dataframe()
    
    def get_essays_from_pbs(self):
        updateat_filter: str = f"AND ((e.UpdatedAt is not null AND e.UpdatedAt > '{self.max_update_at_sink}') OR (e.UpdatedAt is null AND e.CreatedAt > '{self.max_update_at_sink}'))"

        query = f"""
          --CONSULTA DE ENSAIOS e COORDENADAS
          SELECT
            --info ensayo
            e.Id AS IdEnsayo,
            em.Nombre AS NombreEnsayo,
            e.IdAnio,
            p.Nombre as pais,
            sr.Nombre as micro,
            mr.Nombre as macro,
            e.idTipoSemilla as TipoSemilla,
            --LOCALIDADE
            lo.Nombre as localidad,
            lo.Latitud AS LocalidadLatitud,
            lo.Longitud AS LocalidadLongitud,
            --ESTABELECIMENTO
            est.Nombre AS establecimiento,
            est.Latitud AS EstablecimientoLatitud,
            est.Longitud AS EstablecimientoLongitud,
            --LOTE
            lot.Nombre AS lote,
            lot.Latitud AS LoteLatitud, 
            lot.Longitud AS LoteLongitud,
            --DatosComplementarios - Ensaio	
            endcLat.Valor AS EnsayoLatitud,
            endcLon.Valor AS EnsayoLongitud,
            --Modificaciones
            fe.Fecha as FechaDeSiembra,
            e.CreatedAt,
            e.UpdatedAt
          FROM 
            bronzepbsinvestigacion.Ensayo e
                INNER JOIN bronzepbsinvestigacion.EstadoMaterial em
                    ON em.Id = e.IdEstadoMaterial
                INNER JOIN bronzepbsinvestigacion.Localidad lo
                    ON e.IdLocalidad = lo.Id
                INNER JOIN bronzepbsinvestigacion.SubRegion sr
                    ON lo.IdSubRegion = sr.Id
                INNER JOIN bronzepbsinvestigacion.MacroRegion mr
                    ON sr.IdMacroRegion = mr.Id
                INNER JOIN bronzepbsinvestigacion.Pais p
                    ON mr.IdPais = p.Id
                LEFT JOIN bronzepbsinvestigacion.FechaEnsayo FE
                    ON E.Id = FE.IdEnsayo AND FE.Tipo = 1 AND FE.Activo = 1
            -- Merge para obtener latlon de Establecimiento y Lote
              LEFT JOIN (
                  SELECT
                      *
                  FROM
                      bronzepbsinvestigacion.EnsayoDatoComplementario
              WHERE 
                      IdDatoComplementario = 1 AND Activo = 1
            ) enest ON e.Id = enest.IdEnsayo
            LEFT JOIN (
                  SELECT 
                      *
                  FROM
                      bronzepbsinvestigacion.EnsayoDatoComplementario
               WHERE
                      IdDatoComplementario = 2 AND Activo = 1
              ) enlot ON e.Id = enlot.IdEnsayo
            LEFT JOIN bronzepbsinvestigacion.Lote lot
                  ON enlot.Valor = lot.Id
            LEFT JOIN bronzepbsinvestigacion.Establecimiento est
                  ON enest.Valor = est.Id
            --Merge para obter latlon de ensaio
              LEFT JOIN bronzepbsinvestigacion.EnsayoDatoComplementario endcLat
                  ON endcLat.IdEnsayo = e.Id
                AND endcLat.IdDatoComplementario = 73
                AND endcLat.Activo = 1
            LEFT JOIN bronzepbsinvestigacion.EnsayoDatoComplementario endcLon
                ON endcLon.IdEnsayo = e.Id
                AND endcLon.IdDatoComplementario = 74
                AND endcLon.Activo = 1
            WHERE 
              e.Activo = 1
                AND em.Activo = 1
                    AND lo.Activo = 1
                        AND sr.Activo = 1
                            AND mr.Activo = 1
              AND p.Nombre in ('USA', 'ARGENTINA', 'BRASIL', 'URUGUAY', 'PARAGUAY')
              {updateat_filter if self.max_update_at_sink else ''}
        """
        print('Retrieving essays data from PBS database')

        self.df = spark.sql(query).toPandas()
        print(f'\t {self.df.shape[0]} essays has been fetched')

    def check_if_dataframe_has_required_schema(self) -> None:
        print('Check - Mandatory & unnecessary fields')
        
        df_columns: List[str] = self.df.columns.tolist()
        missing_mandatory_fields: Set[str] = set(REQUIRED_SCHEMA_FOR_QUERY).difference(df_columns)
        extra_fields: Set[str] = set(df_columns).difference(REQUIRED_SCHEMA_FOR_QUERY)

        if missing_mandatory_fields:
            raise ValueError(f'The following columns are mandatory: {missing_mandatory_fields}')
        elif extra_fields:
            print('\t Extra fields has been discovered, dropping...')
            self.df.drop(extra_fields, axis=1, inplace=True)
        
        print('\t All fields are as required')

    def pre_process_essays_dataframe(self) -> None:
        print('Pre-processing')
        
        print('\t Dropping duplicated rows')
        self.df.drop_duplicates(inplace=True)

        print('\t Casting numeric fields to float/int')
        self.df[COORDINATES_FIELDS_FLAT] = self.df[COORDINATES_FIELDS_FLAT].apply(pd.to_numeric)
        self.df['IdAnio'] = self.df['IdAnio'].apply(pd.to_numeric)
        
        print('\t Replacing None and Zero values for NaN')
        self.df.fillna(np.nan, inplace=True)
        self.df.replace(0, np.nan, inplace=True)

        print('\t Creating new columns to allocate final pair of coordinates')
        self.df[FINAL_LONGITUDE_COLUMN] = np.nan
        self.df[FINAL_LATITUDE_COLUMN] = np.nan
        
        print('\t Creating new column to allocate source of final pair of coordinates')
        self.df['fuente_de_coordenadas'] = np.nan

    def drop_rows_without_any_coordinate(self) -> None:
        print('Clean up - Dropping essays without any values on any coordinate field')
        
        prev_row_count: int = self.df.shape[0]
        self.df.dropna(subset=COORDINATES_FIELDS_FLAT, how='all', inplace=True)
        post_row_count: int = self.df.shape[0]

        if self.df.empty:
            raise NoValuesError('After deleting rows without any coordinate there are no further essays to analyze. Terminating the execution')
            
        print(f'\t {prev_row_count - post_row_count} essays has been discarded')

    def drop_rows_with_empty_mandatory_values(self) -> None:
        print('Clean up - Dropping essays with at least one mandatory field with null value')
        
        prev_row_count: int = self.df.shape[0]
        self.df.dropna(subset=NON_NULLABLE_FIELDS, how='any', inplace=True)
        post_row_count: int = self.df.shape[0]

        if self.df.empty:
            raise NoValuesError('After deleting rows with at least one mandatory field without data there are no further essays to analyze. Terminating the execution')
            
        print(f'\t {prev_row_count - post_row_count} essays has been discarded')

    def choose_final_pair_of_coordinates(self) -> None:
        """
            This function will fill the final pair of coordinates.\n
            This function will operate with inplace operations (no copies will be created) on the instance's dataframe.\n
            In order to do so, for every level of coordinates (in the order: 'ensayo'->'lote'->'establecimiento'->'localidad')
            starting with the highest priority, it will check all rows (essays) with both of the components for the pair of
            coordinates for the current level, that also happens to not have been set with a final pair.
            It it so, assign the coordinates for the current level to the final pair and set the column 'fuente_de_coordenadas'
            with the name of the current level.

            Raises:
                CoordinatesError: In case that any essay has not been set with a final pair of coordinates, or
                    has been set with an incomplete pair of coordinates.
        """
        print('Choosing final pair of coordinates from all 4 levels')
        FINAL_COORDINATES_COLUMNS: Iterable[str, str] = [FINAL_LONGITUDE_COLUMN, FINAL_LATITUDE_COLUMN]

        # For each level of coordinates (from higher to lower priority)
        for level in COORDINATES_FIELDS:
            print(f'\t Filling values with coordinates on level: {level}')
            current_level_columns: Sequence[str, str] = COORDINATES_FIELDS[level]

            # Draw a mask to select all registers in this level that has not been set with final coordinates and, the coordinates for this level exists
            mask_all_null_coords: pd.Series = (~self.df[current_level_columns].isnull().any(axis=1) & self.df[FINAL_COORDINATES_COLUMNS].isnull().any(axis=1))

            # If at least one match has been detected in the previous mask, fill final coordinates with current level's pair and mark source with current level name
            if any(mask_all_null_coords):
                self.df.loc[mask_all_null_coords, FINAL_COORDINATES_COLUMNS] = self.df.loc[mask_all_null_coords, current_level_columns].values
                self.df.loc[mask_all_null_coords, 'fuente_de_coordenadas'] = level

        # If there are still cases with empty final coordinates, terminate execution
        still_empty_rows = any(self.df[FINAL_COORDINATES_COLUMNS].isnull().any(axis=1))
        if still_empty_rows:
            raise CoordinatesError('Some essays has not been set with a final pair of coordinates')
            
    def discard_out_of_range_coordinates(self) -> None:
        print('Dropping essays with coordinate values outside the proper range of values')
        
        # Draw masks to find essays with coordinate values out of range
        mask_longitudes_with_bad_values: pd.Series = ((self.df[FINAL_LONGITUDE_COLUMN] >= -180) & (self.df[FINAL_LONGITUDE_COLUMN] <= 180))
        mask_latitudes_with_bad_values: pd.Series = ((self.df[FINAL_LATITUDE_COLUMN] >= -90) & (self.df[FINAL_LATITUDE_COLUMN] <= 90))
        
        # Invert the mask to catch essays with bad coordinates
        indexes_to_delete: Int64Index = self.df.loc[~(mask_latitudes_with_bad_values & mask_longitudes_with_bad_values), :].index
        
        # Discard essays with improper coordinate values
        self.df.drop(index=indexes_to_delete, inplace=True)
        
        print(f'\t {len(indexes_to_delete)} of essays were dropped due to wrong values in final latitude and longitude fields')

    def fix_exchanged_coordinates(self) -> None:
        print('Fixing exchanged coordinates')
        
        # Copy values to preserve exchanged data
        self.df['lat_copy'] = self.df[FINAL_LATITUDE_COLUMN].copy()
        # Draw a mask to identify essays with exchanged coordinates
        mask_exchange_coordinates: pd.Series = self.df[FINAL_LONGITUDE_COLUMN] > self.df[FINAL_LATITUDE_COLUMN]
        
        print(f'\t There are {len(self.df[mask_exchange_coordinates])} essays with exchanged coordinates')
        
        # Exchange coordinates
        self.df.loc[mask_exchange_coordinates, FINAL_LATITUDE_COLUMN] = self.df.loc[mask_exchange_coordinates, FINAL_LONGITUDE_COLUMN].values 
        self.df.loc[mask_exchange_coordinates, FINAL_LONGITUDE_COLUMN] = self.df.loc[mask_exchange_coordinates, 'lat_copy'].values 

        self.df.drop('lat_copy', axis=1, inplace=True)
        
    def fix_bad_signed_coordinates(self) -> None:
        print('Fixing bad signed coordinates based on country')
        
        # For every country and his coordinates signing rules
        for country, rules in COORDINATES_SIGNS_BY_COUNTRY.items():
            print(f'\t Fixing coordinates on country {country}')
            # Draw a mask to select essays in the current country
            country_mask: pd.Series = self.df['pais'] == country
            
            # If essays were found, fix them
            if any(country_mask):
                self.df.loc[country_mask, FINAL_LONGITUDE_COLUMN] = self.df.loc[country_mask, FINAL_LONGITUDE_COLUMN].apply(rules[FINAL_LONGITUDE_COLUMN])
                self.df.loc[country_mask, FINAL_LATITUDE_COLUMN]  = self.df.loc[country_mask, FINAL_LATITUDE_COLUMN].apply(rules[FINAL_LATITUDE_COLUMN])
            else:
                print(f'\t\t [Warning] - The country {country} has no essays on current execution')
                
    def post_process_essays_dataframe(self) -> None:
        print('Post-processing')
        
        print('\t Creating field "FUENTE"')
        self.df['fuente'] = 'PBS' # Value is always hardcoded
        
        print('\t Setting id_download_batch field')
        self.df['id_download_batch'] = CURRENT_ID_DOWNLOAD_BATCH
        
        print('\t Filling null values on column "FechaDeSiembra" with default date')
        self.df.loc[self.df['FechaDeSiembra'].isnull(), 'FechaDeSiembra'] = DEFAULT_FECHA_DE_SIEMBRA
        
        print('\t Merging UpdatedAt and CreatedAt fields')
        mask_update_is_null: pd.Series = self.df['UpdatedAt'].isnull()
        self.df['feat_date'] = pd.NaT
        self.df.loc[mask_update_is_null, 'feat_date'] = self.df.loc[mask_update_is_null, 'CreatedAt'].values
        self.df.loc[~mask_update_is_null, 'feat_date'] = self.df.loc[~mask_update_is_null, 'UpdatedAt'].values

# COMMAND ----------

# MAGIC %md
# MAGIC # Startup

# COMMAND ----------

host='cornerstone-geospatial.postgres.database.azure.com'
dbname='Data_Enrichment'
user='picadm@cornerstone-geospatial'
password='Cmu209Pcb!jza'
    
connectionstring = 'postgresql+psycopg2://{server}:5432/{database}?user={uid}&password={password}'.format(
    uid=urllib.parse.quote_plus(user),
    password=urllib.parse.quote_plus(password),
    server=host,
    database=dbname
)

db_connection = create_engine(connectionstring)

# COMMAND ----------

# jdbcHostname = dbutils.secrets.get(scope='secretgdm-scope', key='PSQLHost')
# jdbcPort = dbutils.secrets.get(scope='secretgdm-scope', key='PSQLPort')
# jdbcDatabase = dbutils.secrets.get(scope='secretgdm-scope', key='PSQLDB')
# user_db = dbutils.secrets.get(scope='secretgdm-scope', key='PSQLUser')
# pass_db = dbutils.secrets.get(scope='secretgdm-scope', key='PSQLPass')

# connectionstring = 'postgresql+psycopg2://{server}:5432/{database}?user={uid}&password={password}'.format(
#     uid=urllib.parse.quote_plus(user_db),
#     password=urllib.parse.quote_plus(pass_db),
#     server=jdbcHostname,
#     database=jdbcDatabase
# )

# db_connection = create_engine(connectionstring)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Retrieve PBS essays

# COMMAND ----------

sql: str = f"""
    SELECT
        MAX("UpdatedAt")
    FROM
        "{DB_QUERY_SCHEMAS['historical']}".essays_pbs;
"""
result: LegacyCursorResult = db_connection.execute(sql)
max_update_at_sink: datetime = result.fetchone()[0]
max_update_at_sink

# COMMAND ----------

if max_update_at_sink:
    # There are essays from previous executions (retrieve partial data based on latest essay on DB)
    max_update_at_sink: str = max_update_at_sink.strftime("%Y-%m-%d %H:%M:%S")
    pbs_data = DFPipeline(max_update_at_sink)
else:
    # First execution (retrieve all results from query)
    pbs_data = DFPipeline()

# COMMAND ----------

if pbs_data.df.empty:
    dbutils.notebook.exit('There are no essays to ingest.')

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Resume of retrieved data

# COMMAND ----------

pbs_data.df.info()

# COMMAND ----------

calplot.calplot(pbs_data.df.groupby('feat_date').size(), cmap='YlGn', colorbar=True, figsize=(20, 8))

# COMMAND ----------

pbs_data.df.groupby('pais').size().reset_index(name='count')

# COMMAND ----------

pbs_data.df.groupby('fuente_de_coordenadas').size().reset_index(name='count')

# COMMAND ----------

gdf_pbs_essays = gpd.GeoDataFrame(pbs_data.df[REQUIRED_SCHEMA_FOR_TABLE], geometry=pbs_data.df.apply(lambda row: shapely.geometry.Point((row['longitud'], row['latitud'])), axis=1), crs="EPSG:4326")

# COMMAND ----------

essays_map = folium.Map(tiles='OpenStreetMap')
gdf_aux = gdf_pbs_essays.drop_duplicates('geometry')

for _, row in gdf_aux.iterrows():
    location: Tuple[float, float] = (row['geometry'].y, row['geometry'].x)
    essays_map.add_child(
        folium.Marker(
            location=location,
            popup=f"Location: {location}<br>",
            icon=folium.Icon(color="green")
        )
    )

# COMMAND ----------

essays_map

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Upload data to database

# COMMAND ----------

sql = f"""
    TRUNCATE TABLE "{DB_QUERY_SCHEMAS['incremental']}".essays_incremental_PBS
"""
db_connection.execute(sql)

# COMMAND ----------

gdf_pbs_essays.to_postgis(
    con=db_connection,
    schema=DB_QUERY_SCHEMAS['incremental'],
    name="essays_incremental_pbs",
    if_exists="append"
)

# COMMAND ----------

sql = f"""   
    INSERT INTO "{DB_QUERY_SCHEMAS['historical']}".essays_PBS
        SELECT
            *
        FROM
            "{DB_QUERY_SCHEMAS['incremental']}".essays_incremental_pbs 
    ON CONFLICT ON CONSTRAINT essays_pbs_pkey
    DO 
        UPDATE SET 
            "IdEnsayo" = EXCLUDED."IdEnsayo",
            "NombreEnsayo" = EXCLUDED."NombreEnsayo",
            "IdAnio" = EXCLUDED."IdAnio",
            pais = EXCLUDED.pais,
            micro = EXCLUDED.micro,
            macro = EXCLUDED.macro,
            "TipoSemilla" = EXCLUDED."TipoSemilla",
            localidad = EXCLUDED.localidad,
            "LocalidadLatitud" = EXCLUDED."LocalidadLatitud",
            "LocalidadLongitud" = EXCLUDED."LocalidadLongitud",
            establecimiento = EXCLUDED.establecimiento,
            "EstablecimientoLatitud" = EXCLUDED."EstablecimientoLatitud",
            "EstablecimientoLongitud" = EXCLUDED."EstablecimientoLongitud",
            lote = EXCLUDED.lote,
            "LoteLatitud" = EXCLUDED."LoteLatitud",
            "LoteLongitud" = EXCLUDED."LoteLongitud",
            "EnsayoLatitud" = EXCLUDED."EnsayoLatitud",
            "EnsayoLongitud" = EXCLUDED."EnsayoLongitud",
            "FechaDeSiembra" = EXCLUDED."FechaDeSiembra",
            "CreatedAt" = EXCLUDED."CreatedAt",
            "UpdatedAt" = EXCLUDED."UpdatedAt",
            latitud = EXCLUDED.latitud,
            longitud = EXCLUDED.longitud,
            fuente = EXCLUDED.fuente,
            fuente_de_coordenadas = EXCLUDED.fuente_de_coordenadas,
            id_download_batch = EXCLUDED.id_download_batch,
            geometry = EXCLUDED.geometry
        WHERE
            "{DB_QUERY_SCHEMAS['historical']}".essays_PBS."UpdatedAt" <> EXCLUDED."UpdatedAt" -- To prevent 'empty updates' hits to db;
"""

db_connection.execute(sql)

# COMMAND ----------

sql = f"""
    INSERT INTO "{DB_QUERY_SCHEMAS['historical']}".essays ("IdEnsay", geometry, "FchDSmb", "NmbrEns", "IdAnio", "LclddNm", "SbRgnNm", "McrRgnN", "PasNmbr", "Altitud", "Fuente", "NmbrRsp", lat, lon, "CreatedAt", "UpdatedAt", "idDownloadBatch")
        SELECT
            "IdEnsayo",
            geometry,
            "FechaDeSiembra",
            "NombreEnsayo",
            "IdAnio",
            localidad,
            micro,
            macro,
            pais,
            0 as altitud,
            fuente,
            'pbs_ingest_automation' as nombre_responsable,
            cast(latitud as float8),
            cast(longitud as float8),
            "CreatedAt",
            "UpdatedAt",
            id_download_batch
        from
            "{DB_QUERY_SCHEMAS['incremental']}".essays_incremental_pbs
    ON CONFLICT ON CONSTRAINT essays_pkey
    DO
        UPDATE SET
            "IdEnsay" = EXCLUDED."IdEnsay",
            geometry = EXCLUDED.geometry,
            "FchDSmb" = EXCLUDED."FchDSmb",
            "NmbrEns" = EXCLUDED."NmbrEns",
            "IdAnio" = EXCLUDED."IdAnio",
            "LclddNm" = EXCLUDED."LclddNm",
            "SbRgnNm" = EXCLUDED."SbRgnNm",
            "McrRgnN" = EXCLUDED."McrRgnN",
            "PasNmbr" = EXCLUDED."PasNmbr",
            "Altitud" = EXCLUDED."Altitud",
            "Fuente" = EXCLUDED."Fuente",
            "NmbrRsp" = EXCLUDED."NmbrRsp",
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            "CreatedAt" = EXCLUDED."CreatedAt",
            "UpdatedAt" = EXCLUDED."UpdatedAt",
            "idDownloadBatch" = EXCLUDED."idDownloadBatch"
        WHERE
            "{DB_QUERY_SCHEMAS['historical']}".essays."UpdatedAt" <> EXCLUDED."UpdatedAt" -- To prevent 'empty updates' hits to db;
"""

db_connection.execute(sql)
