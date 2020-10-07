#!/bin/bash

while getopts s:o: arg
do
        case $arg in
             s)
                STUDY_ID=${OPTARG}
                ;;
             o)
                ORGANISM=${OPTARG}
                ;;
             ?)
            echo "unkonw argument"
        exit 1
        ;;
        esac
done

echo $STUDY_ID
echo $ORGANISM

ftpdir=/net/isilonP/public/rw/homes/tc_cm01/ftp_public/derived/pathways/
STUDYPATH=$ftpdir$STUDY_ID"/fella/"
echo $STUDYPATH

if [ ! -d $STUDYPATH ]; then
  mkdir -p $STUDYPATH;
fi

#module load r-3.6.3-gcc-9.3.0-yb5n44y
#module load pandoc-2.7.3-gcc-9.3.0-gctut72

Rscript /net/isilonP/public/rw/homes/tc_cm01/metabolights/software/ws-py/dev/MtblsWS-Py/resources/fella.R $STUDY_ID $ORGANISM $STUDYPATH
