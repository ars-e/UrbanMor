#!/bin/bash
# Download Manhattan building footprints using AWS CLI (no auth required - public bucket)
#
# S3 Bucket: s3://us-west-2.opendata.source.coop/vida/google-microsoft-osm-open-buildings/
# Manhattan S2 cells (level 4):
#   - 9714264396238159872
#   - 9714266595261415424
#   - 9714268794284670976
#   - 9714270993307926528

set -e  # Exit on error

# Output directory
OUTPUT_DIR="manhattan_buildings"
mkdir -p "$OUTPUT_DIR"

# S3 base path
S3_BUCKET="s3://us-west-2.opendata.source.coop/vida/google-microsoft-osm-open-buildings/geoparquet/by_country_s2/country_iso=USA"

# Manhattan S2 cell IDs
CELLS=(
    "9714264396238159872"
    "9714266595261415424"
    "9714268794284670976"
    "9714270993307926528"
)

echo "================================================================================================="
echo "Manhattan Building Footprints Downloader (S3)"
echo "================================================================================================="
echo ""
echo "Downloading from S3 bucket (public, no credentials required)"
echo "Source: $S3_BUCKET"
echo "Output: $OUTPUT_DIR"
echo ""

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "❌ AWS CLI is not installed"
    echo ""
    echo "Install AWS CLI:"
    echo "  macOS: brew install awscli"
    echo "  Ubuntu: sudo apt install awscli"
    echo "  Or: pip install awscli"
    echo ""
    exit 1
fi

# Configure AWS CLI to allow anonymous access (no credentials needed for public buckets)
export AWS_NO_SIGN_REQUEST=true

echo "Downloading ${#CELLS[@]} parquet files..."
echo ""

SUCCESS_COUNT=0
FAIL_COUNT=0

for CELL_ID in "${CELLS[@]}"; do
    echo "Downloading S2 cell: $CELL_ID"
    OUTPUT_FILE="$OUTPUT_DIR/${CELL_ID}.parquet"
    S3_PATH="${S3_BUCKET}/${CELL_ID}.parquet"

    if [ -f "$OUTPUT_FILE" ]; then
        echo "  ✓ Already exists: $OUTPUT_FILE"
        ((SUCCESS_COUNT++))
    else
        if aws s3 cp "$S3_PATH" "$OUTPUT_FILE" --no-sign-request --region us-west-2; then
            FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
            echo "  ✓ Downloaded: $OUTPUT_FILE ($FILE_SIZE)"
            ((SUCCESS_COUNT++))
        else
            echo "  ✗ Failed to download: $CELL_ID"
            ((FAIL_COUNT++))
        fi
    fi
    echo ""
done

echo "================================================================================================="
echo "DOWNLOAD SUMMARY"
echo "================================================================================================="
echo "Successfully downloaded: $SUCCESS_COUNT files"
echo "Failed downloads: $FAIL_COUNT files"
echo ""
echo "Files saved to: $(pwd)/$OUTPUT_DIR"
echo ""

if [ $SUCCESS_COUNT -gt 0 ]; then
    echo "================================================================================================="
    echo "NEXT STEPS"
    echo "================================================================================================="
    echo ""
    echo "Load and explore the data with Python:"
    echo ""
    echo "  python explore_manhattan_buildings.py"
    echo ""
    echo "Or use GeoPandas directly:"
    echo ""
    echo "  import geopandas as gpd"
    echo "  gdf = gpd.read_parquet('$OUTPUT_DIR/9714264396238159872.parquet')"
    echo "  manhattan = gdf.cx[-74.0479:-73.9067, 40.6795:40.882]"
    echo "  print(f'Buildings in Manhattan: {len(manhattan)}')"
    echo ""
fi
