import pandas as pd
import argparse
import s3fs
fs = s3fs.S3FileSystem(anon=False)

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", type=str, required=True,
                    help="Original CSV data with clients.")
parser.add_argument("-o", "--output", type=str, required=True,
                    help="Output name for the generated DataFrame.")
args = parser.parse_args()

clientes_df = pd.read_csv(fs.open(args.input), encoding='latin-1', sep=';',
                          compression='gzip', error_bad_lines=False,
                          warn_bad_lines=False, low_memory=False)

#clientes_df = pd.read_csv('CLIENTE_part.txt.gz', encoding='latin-1', sep=';',
#                          compression='gzip', error_bad_lines=False,
#                          warn_bad_lines=False, low_memory=False)

# A small number of clients have ID0 and ID1
# clientes_df[clientes_df.ID1.notnull()]

# Roaming?
# What do these dates mean and postal codes mean?
# clientes_df[clientes_df.ID_GRUPO_CLIENTE == 'PREROAM']

# ID_NACIONALIDAD:
# 1 = Spanish
# 2 = EU (?)
# 3 = Others

# ID_SEXO has more errors than DE_SEXO. So we create it again.
sex_dict = {'Hombre': 1, 'Mujer': 2, 'Indefinido': 0, 'No documentado': 0}
clientes_df['ID_SEXO'] = clientes_df['DE_SEXO'].map(sex_dict)
clientes_df.drop('DE_SEXO', inplace=True, axis=1)

# mostly empty:
# DE_SITUAC_LABORAL
# DE_PROFESION
# DE_OCUPACION

# Redundant:
# DE_SERVICIO_PRE_POS
# ID_CODIGO_POSTAL_POBLACION
# ID_FECHA_NACIMIENTO
# CODIGOPOSTAL
clientes_df.drop(['DE_SITUAC_LABORAL', 'DE_PROFESION', 'DE_OCUPACION',
                 'DE_SERVICIO_PRE_POS', 'ID_CODIGO_POSTAL_POBLACION',
                 'ID_FECHA_NACIMIENTO', 'CODIGOPOSTAL'], inplace=True, axis=1)

clientes_df.FE_NACIMIENTO = pd.to_numeric(clientes_df.FE_NACIMIENTO,
                                          errors='coerce').fillna(-1).astype(int)

clientes_df.columns = ['ID0', 'ID1', 'ID_Service', 'Country', 'Postal_Code',
                       'Age', 'Client Group', 'City', 'Province',
                       'Year_Of_Birth', 'ID_Country_Group', 'Sex']

# Save to file
output_name = args.output
if not output_name.endswith('.gz'):
    output_name += '.gz'
clientes_df.to_csv(output_name, compression='gzip', index=False)
