import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Albums
Albums_node1744448445733 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "multiLine": "true", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-deproject/Staging/albums (1).csv"], "recurse": True}, transformation_ctx="Albums_node1744448445733")

# Script generated for node Tracks
Tracks_node1744448447643 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "multiLine": "true", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-deproject/Staging/track.csv"], "recurse": True}, transformation_ctx="Tracks_node1744448447643")

# Script generated for node Artist
Artist_node1744448446719 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "multiLine": "true", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-deproject/Staging/artists.csv"], "recurse": True}, transformation_ctx="Artist_node1744448446719")

# Script generated for node Join of Artists and Albums
JoinofArtistsandAlbums_node1744448577546 = Join.apply(frame1=Artist_node1744448446719, frame2=Albums_node1744448445733, keys1=["id"], keys2=["artist_id"], transformation_ctx="JoinofArtistsandAlbums_node1744448577546")

# Script generated for node Join of Artist, Albums & Tracks
JoinofArtistAlbumsTracks_node1744448686456 = Join.apply(frame1=Tracks_node1744448447643, frame2=JoinofArtistsandAlbums_node1744448577546, keys1=["track_id"], keys2=["track_id"], transformation_ctx="JoinofArtistAlbumsTracks_node1744448686456")

# Script generated for node Drop Fields
DropFields_node1744448809943 = DropFields.apply(frame=JoinofArtistAlbumsTracks_node1744448686456, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1744448809943")

# Script generated for node Destination
EvaluateDataQuality().process_rows(frame=DropFields_node1744448809943, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1744448411352", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Destination_node1744448966789 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1744448809943, connection_type="s3", format="glueparquet", connection_options={"path": "s3://spotify-deproject/Data Warehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1744448966789")

job.commit()