How to use
```bash
docker pull bardend123/db-bienes-servicios:v1

# 2. Ejecutar el contenedor
docker run -d \
  --name db-bienes-servicios \
  -p 5432:5432 \
  bardend123/db-bienes-servicios:v1

```

```bash
docker exec -it db-bienes-servicios_postgres psql -U admin -d db_bienes_servicios

db_bienes_servicios=# \d bienes_servicios
                              Table "public.bienes_servicios"
           Column           |            Type             | Collation | Nullable | Default 
----------------------------+-----------------------------+-----------+----------+---------
 FechaCorte                 | timestamp without time zone |           |          | 
 NumeroRuc                  | bigint                      |           |          | 
 FechaOrden                 | timestamp without time zone |           |          | 
 NumeroOrden                | bigint                      |           |          | 
 CodigoGenerico             | text                        |           |          | 
 DescripcionGenerico        | text                        |           |          | 
 Importe                    | double precision            |           |          | 
 ExpedienteSiaf             | bigint                      |           |          | 
 FuenteFinanciamiento       | text                        |           |          | 
 EspecificoGasto            | bigint                      |           |          | 
 DescripcionEspecificoGasto | text                        |           |          | 
 Cantidad                   | double precision            |           |          | 
 TipoBien                   | text                        |           |          | 
 TipoRecurso                | text                        |           |          | 
 FuncionPrograma            | text                        |           |          | 
 MetaSiaf                   | bigint                      |           |          | 
 Programa                   | bigint                      |           |          | 
 Subprograma                | bigint                      |           |          | 
 ```

 ```bash
  2024-04-25 00:00:00 | 10412000841 | 2024-02-09 00:00:00 |           5 | B89            | TELAS Y MATERIALES TEXTILES: INCLUYE VESTUARIO               |       6000 |           1146 | 18 CANON Y SOBRdb_bienes_servicios=# SELECT "FechaOrden", "NumeroOrden", "Importe"
FROM bienes_servicios 
LIMIT 15;
     FechaOrden      | NumeroOrden | Importe 
---------------------+-------------+---------
 2024-02-02 00:00:00 |           1 |  392168
 2024-02-02 00:00:00 |           2 |    4500
 2024-02-02 00:00:00 |           2 |   18000
 2024-02-02 00:00:00 |           2 |    5700
 2024-02-02 00:00:00 |           2 |    2500
 2024-02-02 00:00:00 |           2 |    5850
 2024-02-02 00:00:00 |           2 |    2700
 2024-02-08 00:00:00 |           3 |    6250
 2024-02-08 00:00:00 |           3 |    4520
 2024-02-08 00:00:00 |           3 |    4800
 2024-02-08 00:00:00 |           3 |    2000
 2024-02-08 00:00:00 |           3 |   12000
 2024-02-08 00:00:00 |           3 |    6000
 2024-02-09 00:00:00 |           5 |    3500
 ```