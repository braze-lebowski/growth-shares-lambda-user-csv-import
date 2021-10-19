#!/bin/sh
version="0.1.3"

echo "Creating build directory"

if [ -d "./build" ]
then 
    echo "Build directory exists, skipping"
else
    mkdir build
fi

echo "Packaging depencies"
cd braze_user_csv_import
pip install --target ./package requests
echo "Packaging the app"
cd package
zip -r ../braze-lambda-user-csv-import-v"$version".zip .
cd ..
zip -g braze-lambda-user-csv-import-v"$version".zip app.py
mv braze-lambda-user-csv-import-v"$version".zip ../build
rm -r package
echo "Done"