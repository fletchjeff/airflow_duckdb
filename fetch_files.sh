# This script will fetch the files from the website and unzip them, then remove the zip files and the readme.html file and then rename .csv the files to remove the parentheses

for year in {2018..2022}
  do 
    for month in {1..12}
        do curl -O -k https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${year}_${month}.zip
    done
done
unzip -o '*.zip' 
rm *.zip
rm readme.html
for file in On_Time_Reporting_Carrier*.csv
    do
        new_file_name=${file//\(/}
        mv $file ${new_file_name//\)/}
done