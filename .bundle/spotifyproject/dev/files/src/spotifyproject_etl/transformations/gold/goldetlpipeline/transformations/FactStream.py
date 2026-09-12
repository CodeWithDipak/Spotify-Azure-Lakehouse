import dlt

#------------------------------
# rules for the staging table
#-----------------------------
rules = {
    "valid_stream_id": "stream_id IS NOT NULL"
}

#----------------------------------------------
# enable row tracking for incremental loading
#----------------------------------------------
@dlt.table(
    table_properties={
        'delta.enableRowTracking': 'true'
    }
)

#-----------------------
# Applied expectations
#-----------------------
@dlt.expect_all_or_drop(rules)

#----------------------------------------
# create staging table from source table
#----------------------------------------
def factstream_stg():
    return spark.readStream.table('silvecatalog.silverschema.factstream')

#--------------------------
# create streaming table
#--------------------------
dlt.create_streaming_table(
    name="factstream_streaming",
    table_properties={
        'delta.enableRowTracking': 'true'
    }
)

#---------------------------------------------------------------------------------
# create auto change data capture(cdc) with slowly changing dimensions(scd) type 2
#----------------------------------------------------------------------------------
dlt.create_auto_cdc_flow(
    source='factstream_stg',
    target= 'factstream_streaming',
    keys=['stream_id'],
    sequence_by='stream_timestamp',
    stored_as_scd_type= 1
)