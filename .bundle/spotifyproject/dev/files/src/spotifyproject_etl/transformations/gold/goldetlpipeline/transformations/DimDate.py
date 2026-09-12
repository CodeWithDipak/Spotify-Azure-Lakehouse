import dlt

#------------------------------
# rules for the staging table
#-----------------------------
rules = {
    "valid_date_key": "date_key IS NOT NULL",
    "valid_date": "date IS NOT NULL"
}

#----------------------------------------------
# enable row tracking for incremental loading
#----------------------------------------------
@dlt.table(
    table_properties={
        'delta.enableRowTracking': 'true'
    }
)

#-----------------------------
# Applied expectations
#-----------------------------
@dlt.expect_all_or_drop(rules)

#----------------------------------------
# create staging table from source table
#----------------------------------------
def dimdate_stg():
    return spark.readStream.table('silvecatalog.silverschema.dimdate')

#--------------------------
# create streaming table
#--------------------------
dlt.create_streaming_table(
    name="dimdate_streaming",
    table_properties={
        'delta.enableRowTracking': 'true'
    }
)

#---------------------------------------------------------------------------------
# create auto change data capture(cdc) with slowly changing dimensions(scd) type 2
#----------------------------------------------------------------------------------
dlt.create_auto_cdc_flow(
    source='dimdate_stg',
    target= 'dimdate_streaming',
    keys=['date_key'],
    sequence_by='date',
    stored_as_scd_type= 2
)