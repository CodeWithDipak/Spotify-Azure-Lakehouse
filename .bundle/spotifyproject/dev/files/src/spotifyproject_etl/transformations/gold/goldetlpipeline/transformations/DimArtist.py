import dlt

#------------------------------
# rules for the staging table
#-----------------------------
rules = {
    "valid_artist_id": "artist_id IS NOT NULL",
    "valid_artist_name": "artist_name IS NOT NULL"
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
def dimartist_stg():
    return spark.readStream.table('silvecatalog.silverschema.dimartist')

#--------------------------
# create streaming table
#--------------------------
dlt.create_streaming_table(
    name="dimartist_streaming",
    table_properties={
        'delta.enableRowTracking': 'true'
    }
)

#---------------------------------------------------------------------------------
# create auto change data capture(cdc) with slowly changing dimensions(scd) type 2
#----------------------------------------------------------------------------------
dlt.create_auto_cdc_flow(
    source='dimartist_stg',
    target= 'dimartist_streaming',
    keys=['artist_id'],
    sequence_by='updated_at',
    stored_as_scd_type= 2
)