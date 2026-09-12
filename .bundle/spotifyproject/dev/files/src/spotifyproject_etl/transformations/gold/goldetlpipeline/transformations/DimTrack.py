import dlt

#------------------------------
# rules for the staging table
#-----------------------------
rules = {
    "valid_track_id": "track_id IS NOT NULL",
    "valid_track_name": "track_name IS NOT NULL"
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
def dimtrack_stg():
    return spark.readStream.table('silvecatalog.silverschema.dimtrack')

#--------------------------
# create streaming table
#--------------------------
dlt.create_streaming_table(
    name="dimtrack_streaming",
    table_properties={
        'delta.enableRowTracking': 'true'
    }
)

#---------------------------------------------------------------------------------
# create auto change data capture(cdc) with slowly changing dimensions(scd) type 2
#----------------------------------------------------------------------------------
dlt.create_auto_cdc_flow(
    source='dimtrack_stg',
    target= 'dimtrack_streaming',
    keys=['track_id'],
    sequence_by='updated_at',
    stored_as_scd_type= 2
)