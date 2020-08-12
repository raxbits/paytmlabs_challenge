from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType

ext='.csv'
data_path='./data/2019/'
station_file = './stationlist{}'.format(ext)
country_file = './countrylist{}'.format(ext)

def getPartions(data=None):
    #get *.gz files in the data path
    if not data:
        return []

    from os import walk
    files = []
    for (dirpath, dirnames, filenames) in walk(data):
        files.extend(filenames)
        break
    print(files)
    files = [data_path + s for s in files]
    return files

# initialize spark ctx
def initialize_spark(): 
    '''
    init build and return spark ctx.
    '''
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Paytm Test") \
        .getOrCreate()

    print("spark init success.", "\n")
    return spark
#load with schema, hopefully from repository later.
def gen_schema(spark,files):
    schema = StructType([
        StructField("STN---", IntegerType(), False),
        StructField("WBAN", IntegerType(), False),
        StructField("YEARMODA", IntegerType(), False),
        StructField("TEMP", FloatType(), False),
        StructField("DEWP", FloatType(), False),
        StructField("SLP", FloatType(), False),
        StructField("STP", FloatType(), False),
        StructField("VISIB", FloatType(), False),
        StructField("WDSP", FloatType(), False),
        StructField("MXSPD", FloatType(), False),
        StructField("GUST", FloatType(), False),
        StructField("MAX", FloatType(), False),
        StructField("MIN", FloatType(), False),
        StructField("PRCP", FloatType(), False),
        StructField("SNDP", FloatType(), False),
        StructField("FRSHTT", StringType(), False)
    ])
    
    df = spark \
        .read \
        .format("csv") \
        .schema(schema)         \
        .option("header", "true") \
        .load(files)
    return df
    
scSpark = initialize_spark()

files = getPartions(data=data_path)

missing_array_map = ['',9.99,99.99,999.99,9999.99] #missing indicators

df = gen_schema(scSpark,files)
#loop through items that is not permissible and filter df systematically
for col_name in df.schema.names:
    for missing in missing_array_map:
        df = df.filter((df[col_name] != missing))

station_df = scSpark.read.csv(station_file, header= True,sep=',').cache()
country_df = scSpark.read.csv(country_file, header= True,sep=',').cache()
df = df.selectExpr("STN--- as STN_NO") #rename colomn for join
df_comb = station_df.join(country_df, on=['COUNTRY_ABBR'], how='inner').drop('COUNTRY_ABBR').cache()
df_comb = df.join(df_comb, on=['STN_NO'], how='inner').cache()

#data should be enough to ask questions to now, but had no time to complete the questions.


