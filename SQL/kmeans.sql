
CREATE OR REPLACE FUNCTION kmeans(text, centers integer[], iter_max integer default 10, nstart integer default 1, algorithm text default 'Hartigan-Wong', trace boolean default false)
RETURNS jsonb AS

$$


  df <- pg.spi.exec(arg1)


modelo <- kmeans(df,
                 centers,
                 iter_max,
                 nstart,
                 algorithm,
                 trace
)

class(modelo)<- 'list'

modelo <- jsonlite::serializeJSON(modelo)

return(modelo)

$$
  language plr;
