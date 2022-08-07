spark-submit \
    --master yarn \
    --deploy-mode cluster \
    a2.py \
    --output $1
    --yarn